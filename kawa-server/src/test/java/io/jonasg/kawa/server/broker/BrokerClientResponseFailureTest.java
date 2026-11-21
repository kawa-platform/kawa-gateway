package io.jonasg.kawa.server.broker;

import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.core.Interceptor;
import io.jonasg.kawa.core.InterceptorPipeline;
import io.jonasg.kawa.core.Response;
import io.jonasg.kawa.core.metrics.GatewayMetrics;
import io.jonasg.kawa.protocol.kafka.KafkaApiRegistry;
import io.jonasg.kawa.protocol.kafka.KafkaBodyCodec;
import io.jonasg.kawa.protocol.kafka.KafkaClientRequest;
import io.jonasg.kawa.protocol.kafka.KafkaFrameDecoder;
import io.jonasg.kawa.protocol.kafka.KafkaFrameEncoder;
import io.jonasg.kawa.protocol.kafka.KafkaHeader;
import io.jonasg.kawa.protocol.kafka.ResponseHeaderCodec;
import io.jonasg.kawa.server.netty.ClientSession;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/// A response an interceptor fails on costs only the client that sent the request: the broker
/// connection it shares with other clients stays open and their responses are delivered.
class BrokerClientResponseFailureTest {

    private static final short API_VERSION = 3;

    private EventLoopGroup group;
    private KafkaBodyCodec codec;
    private final ResponseHeaderCodec responseHeaderCodec = new ResponseHeaderCodec();

    @BeforeEach
    void setUp() {
        group = new NioEventLoopGroup(1);
        codec = new KafkaBodyCodec(KafkaApiRegistry.create());
    }

    @AfterEach
    void tearDown() {
        group.shutdownGracefully();
    }

    @Test
    void answersTheOtherClientsWhenAnInterceptorFailsOnOneClientsResponse() {
        // given a broker that answers once both requests are pending, and an interceptor that fails on the first client's response
        var failing = new GatewayContext("failing-client", System.nanoTime());
        var healthy = new GatewayContext("healthy-client", System.nanoTime());
        BrokerClient brokerClient = brokerClient(startBroker(2), failing);
        var failingChannel = new EmbeddedChannel();
        var healthyChannel = new EmbeddedChannel();

        // when
        brokerClient.send(new ClientSession(failingChannel), failing, apiVersions(1));
        brokerClient.send(new ClientSession(healthyChannel), healthy, apiVersions(2));

        // then
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(healthyChannel.outboundMessages())
                .withFailMessage(() -> "The other client sharing the broker connection got no response")
                .isNotEmpty());
        assertThat(brokerClient.connected())
                .withFailMessage(() -> "The broker connection shared by the other clients was closed")
                .isTrue();
    }

    @Test
    void disconnectsTheClientWhoseResponseAnInterceptorFailsOn() {
        // given an interceptor that fails on the client's response
        var failing = new GatewayContext("failing-client", System.nanoTime());
        BrokerClient brokerClient = brokerClient(startBroker(1), failing);
        var failingChannel = new EmbeddedChannel();

        // when
        brokerClient.send(new ClientSession(failingChannel), failing, apiVersions(1));

        // then the client is not left waiting for a response that never comes
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(failingChannel.isOpen())
                .withFailMessage(() -> "The client whose response could not be processed was left connected without an answer")
                .isFalse());
        assertThat(failingChannel.outboundMessages())
                .withFailMessage(() -> "The client whose response could not be processed got a response")
                .isEmpty();
    }

    private BrokerClient brokerClient(Channel broker, GatewayContext failing) {
        Interceptor failsOnResponse = new Interceptor() {
            @Override
            public void onResponse(GatewayContext context, Response response) {
                if (context == failing) {
                    throw new IllegalStateException("interceptor bug");
                }
            }
        };
        int port = ((InetSocketAddress) broker.localAddress()).getPort();
        return new BrokerClient(1, "localhost", port, group, codec, new InterceptorPipeline(List.of(failsOnResponse)),
                new GatewayMetrics(new SimpleMeterRegistry()), BrokerConnectionSettings.PLAINTEXT);
    }

    private static KafkaClientRequest apiVersions(int correlationId) {
        return KafkaClientRequest.of(
                KafkaHeader.of(ApiKeys.API_VERSIONS.id, API_VERSION, correlationId, "test-client"),
                new ApiVersionsRequestData().setClientSoftwareName("test"));
    }

    /// A broker that holds ApiVersions requests until `batch` of them arrived, then answers them in order.
    private Channel startBroker(int batch) {
        var bs = new ServerBootstrap();
        bs.group(group)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new KafkaFrameDecoder());
                        ch.pipeline().addLast(new KafkaFrameEncoder());
                        ch.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                            private final List<Integer> held = new ArrayList<>();

                            @Override
                            public void channelRead(ChannelHandlerContext ctx, Object msg) {
                                ByteBuf buf = (ByteBuf) msg;
                                int correlationId = buf.getInt(buf.readerIndex() + 4);
                                buf.release();
                                held.add(correlationId);
                                if (held.size() == batch) {
                                    held.forEach(id -> respond(ctx, id));
                                    held.clear();
                                }
                            }
                        });
                    }
                });
        try {
            return bs.bind(0).sync().channel();
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    private void respond(ChannelHandlerContext ctx, int correlationId) {
        KafkaHeader header = KafkaHeader.of(ApiKeys.API_VERSIONS.id, API_VERSION, correlationId, "test-broker");
        ByteBuf out = ctx.alloc().buffer();
        responseHeaderCodec.encode(out, header.responseHeaderVersion(), correlationId);
        codec.encodeResponse(API_VERSION, new ApiVersionsResponseData().setErrorCode(Errors.NONE.code()), out);
        ctx.writeAndFlush(out);
    }
}

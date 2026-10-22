package io.jonasg.kawa.server.broker;

import io.jonasg.kawa.config.BrokerAuthConfig;
import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.core.InterceptorPipeline;
import io.jonasg.kawa.core.metrics.GatewayMetrics;
import io.jonasg.kawa.protocol.kafka.KafkaApiRegistry;
import io.jonasg.kawa.protocol.kafka.KafkaBodyCodec;
import io.jonasg.kawa.protocol.kafka.KafkaClientRequest;
import io.jonasg.kawa.protocol.kafka.KafkaFrameDecoder;
import io.jonasg.kawa.protocol.kafka.KafkaFrameEncoder;
import io.jonasg.kawa.protocol.kafka.KafkaHeader;
import io.jonasg.kawa.protocol.kafka.ResponseHeaderCodec;
import io.jonasg.kawa.server.netty.ClientSession;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.SaslAuthenticateResponseData;
import org.apache.kafka.common.message.SaslHandshakeResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class BrokerClientSaslTest {

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
    void authenticatesAfterConnectWhenBrokerAuthConfigPresent() throws Exception {
        // given a SASL-enabled server
        var handshakeReceived = new CountDownLatch(1);
        Channel serverChannel = startSaslServer(handshakeReceived);
        int port = ((InetSocketAddress) serverChannel.localAddress()).getPort();

        // and a BrokerClient with BrokerAuthConfig
        var authConfig = new BrokerAuthConfig("PLAIN", "gateway", "secret");
        var brokerClient = new BrokerClient(
                1, "localhost", port, group, codec, new InterceptorPipeline(List.of()),
                new GatewayMetrics(new io.micrometer.core.instrument.simple.SimpleMeterRegistry()),
                new BrokerConnectionSettings(authConfig, null));

        // when a request triggers connect (which should include SASL)
        var session = clientSession();
        var context = new GatewayContext("test", System.nanoTime());
        var request = KafkaClientRequest.of(
                KafkaHeader.of(ApiKeys.API_VERSIONS.id, (short) 3, 1, "test-client"),
                new ApiVersionsRequestData().setClientSoftwareName("test"));
        brokerClient.send(session, context, request);

        // then the SASL handshake completed and the request was forwarded
        assertThat(handshakeReceived.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(brokerClient.connected()).isTrue();
    }

    @Test
    void skipsSaslWhenBrokerAuthConfigAbsent() throws Exception {
        // given a plain server (no SASL)
        var requestReceived = new CountDownLatch(1);
        Channel serverChannel = startPlainServer(requestReceived);
        int port = ((InetSocketAddress) serverChannel.localAddress()).getPort();

        // and a BrokerClient without BrokerAuthConfig
        var brokerClient = new BrokerClient(
                1, "localhost", port, group, codec, new InterceptorPipeline(List.of()),
                new GatewayMetrics(new io.micrometer.core.instrument.simple.SimpleMeterRegistry()),
                BrokerConnectionSettings.PLAINTEXT);

        // when a request triggers connect
        var session = clientSession();
        var context = new GatewayContext("test", System.nanoTime());
        var request = KafkaClientRequest.of(
                KafkaHeader.of(ApiKeys.API_VERSIONS.id, (short) 3, 1, "test-client"),
                new ApiVersionsRequestData().setClientSoftwareName("test"));
        brokerClient.send(session, context, request);

        // then the request was forwarded without SASL
        assertThat(requestReceived.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(brokerClient.connected()).isTrue();
    }

    private ClientSession clientSession() {
        return new ClientSession(new EmbeddedChannel());
    }

    private Channel startSaslServer(CountDownLatch saslHandshakeReceived) {
        var bs = new ServerBootstrap();
        bs.group(group)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new KafkaFrameDecoder());
                        ch.pipeline().addLast(new KafkaFrameEncoder());
                        ch.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                            @Override
                            public void channelRead(ChannelHandlerContext ctx, Object msg) {
                                ByteBuf buf = (ByteBuf) msg;
                                short apiKey = buf.readShort();
                                buf.resetReaderIndex();

                                if (apiKey == ApiKeys.SASL_HANDSHAKE.id) {
                                    short apiVersion = buf.getShort(buf.readerIndex() + 2);
                                    var response = new SaslHandshakeResponseData()
                                            .setErrorCode(Errors.NONE.code());
                                    respond(ctx, KafkaApiRegistry.SASL_HANDSHAKE, apiVersion, 1, response);
                                    saslHandshakeReceived.countDown();
                                } else if (apiKey == ApiKeys.SASL_AUTHENTICATE.id) {
                                    short apiVersion = buf.getShort(buf.readerIndex() + 2);
                                    var response = new SaslAuthenticateResponseData()
                                            .setErrorCode(Errors.NONE.code());
                                    respond(ctx, KafkaApiRegistry.SASL_AUTHENTICATE, apiVersion, 2, response);
                                } else {
                                    // any other request — respond with ApiVersions for simplicity
                                    short apiVersion = buf.getShort(buf.readerIndex() + 2);
                                    var response = new ApiVersionsResponseData()
                                            .setErrorCode(Errors.NONE.code());
                                    respond(ctx, ApiKeys.API_VERSIONS.id, apiVersion, 3, response);
                                }
                                buf.release();
                            }
                        });
                    }
                });
        try {
            return bs.bind(0).sync().channel();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private Channel startPlainServer(CountDownLatch requestReceived) {
        var bs = new ServerBootstrap();
        bs.group(group)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new KafkaFrameDecoder());
                        ch.pipeline().addLast(new KafkaFrameEncoder());
                        ch.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                            @Override
                            public void channelRead(ChannelHandlerContext ctx, Object msg) {
                                ByteBuf buf = (ByteBuf) msg;
                                short apiKey = buf.readShort();
                                short apiVersion = buf.getShort(buf.readerIndex() + 2);
                                buf.release();

                                if (apiKey == ApiKeys.API_VERSIONS.id) {
                                    var response = new ApiVersionsResponseData()
                                            .setErrorCode(Errors.NONE.code());
                                    respond(ctx, ApiKeys.API_VERSIONS.id, apiVersion, 3, response);
                                    requestReceived.countDown();
                                }
                            }
                        });
                    }
                });
        try {
            return bs.bind(0).sync().channel();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void respond(
            ChannelHandlerContext ctx,
            short apiKey,
            short apiVersion,
            int correlationId,
            Object body
    ) {
        KafkaHeader header = KafkaHeader.of(apiKey, apiVersion, correlationId, "test-server");
        ByteBuf out = ctx.alloc().buffer();
        responseHeaderCodec.encode(out, header.responseHeaderVersion(), correlationId);
        codec.encodeRequest(apiVersion, body, out);
        ctx.writeAndFlush(out);
    }
}

package io.jonasg.kawa.server.broker;

import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.core.InterceptorPipeline;
import io.jonasg.kawa.core.metrics.GatewayMetrics;
import io.jonasg.kawa.protocol.kafka.KafkaApiRegistry;
import io.jonasg.kawa.protocol.kafka.KafkaBodyCodec;
import io.jonasg.kawa.protocol.kafka.KafkaClientRequest;
import io.jonasg.kawa.protocol.kafka.KafkaHeader;
import io.jonasg.kawa.server.netty.ClientSession;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class BrokerClientTargetsTest {

    @Test
    void reachesTheClusterThroughTheSecondBootstrapServerWhenTheFirstIsDown() throws Exception {
        // given - a first bootstrap server that refuses connections, and a live broker behind the second
        int deadPort;
        try (var probe = new ServerSocket(0)) {
            deadPort = probe.getLocalPort();
        }
        var group = new NioEventLoopGroup(1);
        try {
            Channel live = new ServerBootstrap()
                    .group(group)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                        }
                    })
                    .bind("127.0.0.1", 0).sync().channel();
            int livePort = ((InetSocketAddress) live.localAddress()).getPort();
            var client = new BrokerClient(-1,
                    List.of(new Endpoint("127.0.0.1", deadPort), new Endpoint("127.0.0.1", livePort)),
                    group, new KafkaBodyCodec(KafkaApiRegistry.create()), new InterceptorPipeline(List.of()),
                    new GatewayMetrics(new SimpleMeterRegistry()), BrokerConnectionSettings.PLAINTEXT);
            var request = KafkaClientRequest.of(
                    KafkaHeader.of(ApiKeys.API_VERSIONS.id, (short) 3, 1, "test-client"),
                    new ApiVersionsRequestData().setClientSoftwareName("test"));

            // when - a client's request needs a connection to the cluster
            client.send(new ClientSession(new EmbeddedChannel()), new GatewayContext("test", System.nanoTime()), request);

            // then - the request went out over the second bootstrap server
            assertThat(client.connected())
                    .withFailMessage(() -> "The request was not sent: no bootstrap server after port " + deadPort + " answered")
                    .isTrue();
            live.close().sync();
        } finally {
            group.shutdownGracefully().sync();
        }
    }
}

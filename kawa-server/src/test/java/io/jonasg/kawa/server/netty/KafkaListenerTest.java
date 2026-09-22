package io.jonasg.kawa.server.netty;

import io.jonasg.kawa.core.InterceptorPipeline;
import io.jonasg.kawa.core.cluster.MetadataCache;
import io.jonasg.kawa.core.metrics.GatewayMetrics;
import io.jonasg.kawa.protocol.kafka.ApiVersionsResponseBuilder;
import io.jonasg.kawa.protocol.kafka.KafkaApiRegistry;
import io.jonasg.kawa.protocol.kafka.KafkaBodyCodec;
import io.jonasg.kawa.protocol.kafka.SupportedVersions;
import io.jonasg.kawa.server.KafkaClientRequestHandler;
import io.jonasg.kawa.server.LeaderRouter;
import io.jonasg.kawa.server.auth.SaslAuthenticator;
import io.jonasg.kawa.server.broker.BrokerClientPool;
import io.jonasg.kawa.server.broker.MetadataClient;
import io.jonasg.kawa.virtualtopic.FetchSessionRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.jupiter.api.Test;

import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KafkaListenerTest {

    private final KafkaApiRegistry registry = KafkaApiRegistry.create();
    private final KafkaBodyCodec codec = new KafkaBodyCodec(registry);
    private final GatewayMetrics metrics = new GatewayMetrics(new SimpleMeterRegistry());

    @Test
    void bindReturnsBoundPortAndCloseIsSafe() throws Exception {
        // given
        var listener = new KafkaListener(metrics, codec);

        // when
        int boundPort = listener.bind("127.0.0.1", 0);

        // then
        assertThat(boundPort).isGreaterThan(0);
        assertThat(listener.boundPort()).isEqualTo(boundPort);

        // when
        listener.close();
    }

    @Test
    void connectionsAreClosedUntilDispatcherIsInstalled() throws Exception {
        // given
        var listener = new KafkaListener(metrics, codec);
        int boundPort = listener.bind("127.0.0.1", 0);

        // when — connect before a dispatcher exists
        try (var socket = new Socket("127.0.0.1", boundPort)) {
            // then — the server closes the connection immediately
            assertThat(socket.getInputStream().read()).isEqualTo(-1);
        }

        // when — a dispatcher is installed
        listener.installDispatcher(dispatcher());

        // then — a new connection stays open (read times out instead of EOF)
        try (var socket = new Socket("127.0.0.1", boundPort)) {
            socket.setSoTimeout(500);
            assertThatThrownBy(() -> socket.getInputStream().read())
                    .isInstanceOf(SocketTimeoutException.class);
        }

        listener.close();
    }

    private KafkaClientRequestHandler dispatcher() {
        var group = new NioEventLoopGroup(1);
        var cache = new MetadataCache();
        var pipeline = new InterceptorPipeline(List.of());
        var brokerPool = new BrokerClientPool(group, codec, pipeline, metrics, cache, "localhost", 9092);
        var metadataClient = new MetadataClient("localhost", 9092, group, codec, cache, brokerPool, metrics);
        return new KafkaClientRequestHandler(
                codec, new ApiVersionsResponseBuilder(SupportedVersions.from(registry)), pipeline,
                new LeaderRouter(cache), brokerPool, metadataClient, metrics, new FetchSessionRegistry(),
                new SaslAuthenticator());
    }
}

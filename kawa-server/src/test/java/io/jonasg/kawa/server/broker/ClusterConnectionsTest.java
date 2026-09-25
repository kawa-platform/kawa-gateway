package io.jonasg.kawa.server.broker;

import io.jonasg.kawa.core.InterceptorPipeline;
import io.jonasg.kawa.core.cluster.MetadataCache;
import io.jonasg.kawa.core.metrics.GatewayMetrics;
import io.jonasg.kawa.config.BrokerAuthConfig;
import io.jonasg.kawa.protocol.kafka.KafkaApiRegistry;
import io.jonasg.kawa.protocol.kafka.KafkaBodyCodec;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ClusterConnectionsTest {

    @Test
    void closeIsSafeWithoutStart() {
        // given
        var metrics = new GatewayMetrics(new SimpleMeterRegistry());
        var codec = new KafkaBodyCodec(KafkaApiRegistry.create());
        var pipeline = new InterceptorPipeline(List.of());
        var cache = new MetadataCache();
        var connections = new ClusterConnections(codec, pipeline, metrics, cache, "localhost", 9092, null);

        // when
        connections.close();

        // then
        assertThat(connections.pool()).isNotNull();
        assertThat(connections.metadataClient()).isNotNull();
        assertThat(connections.router()).isNotNull();
    }

    @Test
    void enablesTlsForIamBrokerAuthentication() {
        // given
        var metrics = new GatewayMetrics(new SimpleMeterRegistry());
        var codec = new KafkaBodyCodec(KafkaApiRegistry.create());
        var pipeline = new InterceptorPipeline(List.of());
        var cache = new MetadataCache();
        var auth = new BrokerAuthConfig("AWS_MSK_IAM", null, null);

        // when
        var connections = new ClusterConnections(codec, pipeline, metrics, cache, "localhost", 9098, auth);

        // then
        assertThat(connections.tlsEnabled()).isTrue();
        connections.close();
    }
}

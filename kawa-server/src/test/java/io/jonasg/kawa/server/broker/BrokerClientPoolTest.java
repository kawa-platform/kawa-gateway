package io.jonasg.kawa.server.broker;

import io.jonasg.kawa.core.InterceptorPipeline;
import io.jonasg.kawa.core.cluster.BrokerNode;
import io.jonasg.kawa.core.cluster.MetadataCache;
import io.jonasg.kawa.core.cluster.MetadataSnapshot;
import io.jonasg.kawa.core.metrics.GatewayMetrics;
import io.jonasg.kawa.protocol.kafka.KafkaApiRegistry;
import io.jonasg.kawa.protocol.kafka.KafkaBodyCodec;
import io.jonasg.kawa.server.netty.ClientSession;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/// Clients are created lazily and only connect on their first request, so these tests check which
/// client a session gets without a broker.
class BrokerClientPoolTest {

    private static final BrokerNode BROKER_1 = BrokerNode.of(1, "broker-1", 9092, null);
    private static final BrokerNode BROKER_2 = BrokerNode.of(2, "broker-2", 9093, null);

    private final BrokerClientPool pool = new BrokerClientPool(null, new KafkaBodyCodec(KafkaApiRegistry.create()),
            new InterceptorPipeline(List.of()), new GatewayMetrics(new SimpleMeterRegistry()), cacheWith(BROKER_1, BROKER_2),
            "bootstrap", 9092);
    private final ClientSession alice = new ClientSession(new EmbeddedChannel());
    private final ClientSession bob = new ClientSession(new EmbeddedChannel());

    private static MetadataCache cacheWith(BrokerNode... brokers) {
        var cache = new MetadataCache();
        var nodes = new java.util.HashMap<Integer, BrokerNode>();
        for (BrokerNode broker : brokers) {
            nodes.put(broker.id(), broker);
        }
        cache.update(MetadataSnapshot.of(Map.of(), nodes, "cluster-1"));
        return cache;
    }

    @Test
    void reusesOneConnectionPerSessionAndBroker() {
        // when
        var first = pool.forBroker(alice, 1);
        var second = pool.forBroker(alice, 1);

        // then
        assertThat(second)
                .withFailMessage(() -> "A session got a new connection to broker 1 for its second request")
                .isSameAs(first);
    }

    @Test
    void givesEverySessionItsOwnConnectionToABroker() {
        // when
        var alicesConnection = pool.forBroker(alice, 1);
        var bobsConnection = pool.forBroker(bob, 1);

        // then
        assertThat(bobsConnection)
                .withFailMessage(() -> "Two client connections share one connection to broker 1, so a request the"
                        + " broker holds for one would block the other")
                .isNotSameAs(alicesConnection);
    }

    @Test
    void dropsOnlyTheConnectionsOfAClosedSession() {
        // given
        var alicesConnection = pool.forBroker(alice, 1);
        var bobsConnection = pool.forBroker(bob, 1);

        // when
        pool.closeSession(alice);

        // then
        assertThat(pool.forBroker(alice, 1))
                .withFailMessage(() -> "A closed session's connection was kept")
                .isNotSameAs(alicesConnection);
        assertThat(pool.forBroker(bob, 1))
                .withFailMessage(() -> "Closing one session dropped another session's connection")
                .isSameAs(bobsConnection);
    }

    @Test
    void dropsTheConnectionsOfABrokerThatLeftTheCluster() {
        // given
        var toBroker1 = pool.forBroker(alice, 1);
        var toBroker2 = pool.forBroker(alice, 2);

        // when
        pool.prune(List.of(BROKER_1));

        // then
        assertThat(pool.forBroker(alice, 2))
                .withFailMessage(() -> "The connection to broker 2 was kept after it left the cluster")
                .isNotSameAs(toBroker2);
        assertThat(pool.forBroker(alice, 1))
                .withFailMessage(() -> "Pruning dropped the connection to broker 1, which is still in the cluster")
                .isSameAs(toBroker1);
    }

    @Test
    void fallsBackToTheBootstrapNodeForAnUnknownBroker() {
        // when
        var unknown = pool.forBroker(alice, 42);
        var unrouted = pool.forBroker(alice, -1);

        // then
        assertThat(unrouted)
                .withFailMessage(() -> "An unknown broker and the unrouted route got different bootstrap connections")
                .isSameAs(unknown);
    }
}

package io.jonasg.kawa.it;

import org.apache.kafka.clients.admin.AdminClient;
import org.awaitility.Awaitility;
import org.testcontainers.containers.Network;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/// A KRaft Kafka cluster of several nodes, each both broker and controller, on one Testcontainers
/// network. Node ids are `1..n`; node `i` is reachable inside the network as `kafka-i`.
///
/// The internal topics get several partitions, so their leaders (the group and transaction
/// coordinators) are spread over the brokers instead of all landing on one: a test can then pick a
/// group or transactional id whose coordinator is not the lowest broker id.
final class MultiBrokerKafkaCluster implements AutoCloseable {

    private static final DockerImageName IMAGE = DockerImageName.parse("apache/kafka-native:3.8.0");
    private static final String INTERNAL_TOPIC_PARTITIONS = "6";

    private final Network network = Network.newNetwork();
    private final List<KafkaContainer> nodes;

    MultiBrokerKafkaCluster(int size) {
        String voters = IntStream.rangeClosed(1, size)
                .mapToObj(id -> id + "@" + alias(id) + ":9094")
                .collect(Collectors.joining(","));
        this.nodes = IntStream.rangeClosed(1, size)
                .mapToObj(id -> node(id, voters))
                .toList();
    }

    private KafkaContainer node(int id, String voters) {
        return new KafkaContainer(IMAGE)
                .withNetwork(network)
                .withNetworkAliases(alias(id))
                // brokers advertise each other by container hostname: make it the network alias
                .withCreateContainerCmdModifier(cmd -> cmd.withHostName(alias(id)))
                .withEnv("KAFKA_NODE_ID", String.valueOf(id))
                .withEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", voters)
                .withEnv("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", INTERNAL_TOPIC_PARTITIONS)
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_NUM_PARTITIONS", INTERNAL_TOPIC_PARTITIONS);
    }

    private static String alias(int id) {
        return "kafka-" + id;
    }

    /// Starts every node at once (a KRaft node only becomes ready once the quorum is formed) and
    /// waits until each node sees every broker: their views of the cluster converge independently.
    void start() {
        Startables.deepStart(nodes).join();
        for (KafkaContainer node : nodes) {
            try (var admin = AdminClient.create(Map.of("bootstrap.servers", node.getBootstrapServers()))) {
                Awaitility.await(node.getNetworkAliases() + " to see all " + nodes.size() + " brokers")
                        .atMost(Duration.ofSeconds(60))
                        .until(() -> admin.describeCluster().nodes().get(10, TimeUnit.SECONDS).size() == nodes.size());
            }
        }
    }

    /// The host-reachable bootstrap servers of every node, comma-separated.
    String bootstrapServers() {
        return nodes.stream().map(KafkaContainer::getBootstrapServers).collect(Collectors.joining(","));
    }

    @Override
    public void close() {
        nodes.forEach(KafkaContainer::stop);
        network.close();
    }
}

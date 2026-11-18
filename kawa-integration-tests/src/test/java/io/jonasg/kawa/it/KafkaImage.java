package io.jonasg.kawa.it;

import org.testcontainers.utility.DockerImageName;

/// The broker image every Testcontainers [org.testcontainers.kafka.KafkaContainer] in the integration tests runs.
/// It follows the Kafka client version (`kafka.version` in the parent `pom.xml`), so an upgrade is this one line.
final class KafkaImage {

    static final DockerImageName KAFKA_NATIVE = DockerImageName.parse("apache/kafka-native:4.3.1");

    private KafkaImage() {
    }
}

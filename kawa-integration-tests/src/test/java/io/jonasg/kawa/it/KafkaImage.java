package io.jonasg.kawa.it;

import org.testcontainers.utility.DockerImageName;

/// The broker image every Testcontainers [org.testcontainers.kafka.KafkaContainer] in the integration tests runs.
/// It is ahead of the Kafka client (`kafka.version` in the parent `pom.xml`, 4.3.1) on purpose: the
/// `kafka-native` 4.3.1 image intermittently segfaults during startup on amd64, and 4.4.0 does not. Bring the client
/// up to the image when it is upgraded, and never move the image back to 4.3.x.
final class KafkaImage {

    static final DockerImageName KAFKA_NATIVE = DockerImageName.parse("apache/kafka-native:4.4.0");

    private KafkaImage() {
    }
}

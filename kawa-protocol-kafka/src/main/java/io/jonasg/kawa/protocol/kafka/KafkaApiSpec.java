package io.jonasg.kawa.protocol.kafka;

/// Decode capabilities for one Kafka API.
///
/// @param apiKey         kafka api key
/// @param name           human-readable name
/// @param versionRange   the version range this gateway can decode and will advertise to clients
/// @param requestReader  reads request bodies, or `null` to pass requests through
/// @param responseReader reads response bodies, or `null` to pass responses through
public record KafkaApiSpec(
        short apiKey,
        String name,
        VersionRange versionRange,
        MessageReader requestReader,
        MessageReader responseReader) {

}

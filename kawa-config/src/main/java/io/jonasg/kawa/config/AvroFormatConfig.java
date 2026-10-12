package io.jonasg.kawa.config;

/// Avro-encoded payload in the schema registry wire format (a magic byte and schema id ahead of
/// the Avro binary). The schema is looked up in the schema registry of the virtual topic's
/// cluster ([ClusterConfig#schemaRegistry]); filters see the decoded record instead of the raw bytes.
///
/// @param onDecodeError what to do with a record that cannot be decoded - [DecodeErrorPolicy#SKIP] by default
public record AvroFormatConfig(DecodeErrorPolicy onDecodeError) implements PayloadFormatConfig {

    public AvroFormatConfig {
        onDecodeError = onDecodeError == null ? DecodeErrorPolicy.SKIP : onDecodeError;
    }

    public AvroFormatConfig() {
        this(DecodeErrorPolicy.SKIP);
    }
}

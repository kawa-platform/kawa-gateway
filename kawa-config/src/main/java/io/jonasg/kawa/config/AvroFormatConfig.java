package io.jonasg.kawa.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.jspecify.annotations.Nullable;

/// Avro-encoded payload in the schema registry wire format (a magic byte and schema id ahead of
/// the Avro binary). Schemas are looked up in the schema registry of the virtual topic's
/// cluster ([ClusterConfig#schemaRegistry]).
///
/// Decoding always uses the writer schema named by the id in the payload; filters see the
/// decoded record instead of the raw bytes. Encoding a transformed value (as the output of a
/// virtual topic's transformations) writes it with the schema registered under `subject`.
///
/// `subject` and `version` are left out of the JSON while unset, so a decode-only format reads
/// `{"type": "avro", "onDecodeError": "skip"}` as before.
///
/// @param onDecodeError what to do with a record that cannot be decoded - [DecodeErrorPolicy#SKIP] by default
/// @param subject       the registry subject of the schema transformed values are encoded with; required
///                      when this format is a transformation's output, ignored for decoding
/// @param version       the subject version to encode with, `null` for the latest
@JsonInclude(JsonInclude.Include.NON_NULL)
public record AvroFormatConfig(
        DecodeErrorPolicy onDecodeError,
        @Nullable String subject,
        @Nullable Integer version
) implements PayloadFormatConfig {

    public AvroFormatConfig {
        onDecodeError = onDecodeError == null ? DecodeErrorPolicy.SKIP : onDecodeError;
        if (subject != null && subject.isBlank()) {
            throw new IllegalArgumentException("avro 'subject' must not be blank");
        }
        if (version != null && subject == null) {
            throw new IllegalArgumentException("avro 'version' requires a 'subject'");
        }
        if (version != null && version < 1) {
            throw new IllegalArgumentException("avro 'version' must be 1 or higher, got " + version);
        }
    }

    public AvroFormatConfig(DecodeErrorPolicy onDecodeError) {
        this(onDecodeError, null, null);
    }

    public AvroFormatConfig() {
        this(DecodeErrorPolicy.SKIP);
    }
}

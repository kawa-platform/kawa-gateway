package io.jonasg.kawa.virtualtopic.filter;

import io.jonasg.kawa.config.AvroFormatConfig;
import io.jonasg.kawa.config.JsonFormatConfig;
import io.jonasg.kawa.config.PayloadFormatConfig;
import io.jonasg.kawa.config.XmlFormatConfig;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// The [PayloadDecoder] and [PayloadEncoder] of each [PayloadFormatConfig]. The switches are
/// exhaustive over the sealed interface: a new format is a compile error here until it has a codec.
public final class PayloadCodecs {

    private static final Logger log = LoggerFactory.getLogger(PayloadCodecs.class);

    private PayloadCodecs() {
    }

    /// @param avroSchemas the schemas of the physical topic's cluster, for [AvroFormatConfig];
    ///                    `null` when that cluster has no schema registry, in which case every
    ///                    Avro value fails to decode with a [PayloadDecodeException]
    public static PayloadDecoder decoder(PayloadFormatConfig format, @Nullable AvroSchemaResolver avroSchemas) {
        return switch (format) {
            case JsonFormatConfig _ -> new JsonPayloadDecoder();
            case XmlFormatConfig xml -> new XmlPayloadDecoder(xml.root());
            case AvroFormatConfig _ when avroSchemas != null -> new AvroPayloadDecoder(avroSchemas);
            case AvroFormatConfig _ -> {
                log.warn("A virtual topic has an Avro value format but the cluster of its physical topic has no"
                        + " schemaRegistry; its records cannot be decoded and are handled per its onDecodeError policy");
                yield _ -> {
                    throw new PayloadDecodeException(
                            "record value is Avro but the cluster of its physical topic has no schemaRegistry configured");
                };
            }
        };
    }

    /// @param format      an output format, so an [AvroFormatConfig] names its subject and an
    ///                    [XmlFormatConfig] its root (as `VirtualTopicConfig` enforces)
    /// @param avroSubjects the subjects of the physical topic's cluster, for [AvroFormatConfig];
    ///                    `null` when that cluster has no schema registry, in which case every
    ///                    value fails to encode as Avro with a [PayloadEncodeException]
    public static PayloadEncoder encoder(PayloadFormatConfig format, @Nullable AvroSubjectResolver avroSubjects) {
        return switch (format) {
            case JsonFormatConfig _ -> new JsonPayloadEncoder();
            case XmlFormatConfig xml -> new XmlPayloadEncoder(xml.root());
            case AvroFormatConfig avro when avroSubjects != null -> new AvroPayloadEncoder(avroSubjects, avro.subject(), avro.version());
            case AvroFormatConfig _ -> {
                log.warn("A virtual topic transforms values to Avro but the cluster of its physical topic has no"
                        + " schemaRegistry; its records cannot be encoded and are handled per its onDecodeError policy");
                yield _ -> {
                    throw new PayloadEncodeException(
                            "cannot encode as Avro: the cluster of the physical topic has no schemaRegistry configured");
                };
            }
        };
    }
}

package io.jonasg.kawa.config;

import org.jspecify.annotations.Nullable;

import java.util.List;

/// Virtual topic configuration.
///
/// @param topic               physical topic name
/// @param filter              optional server-side consume filter configuration
/// @param exposePhysicalTopic when `true`, the physical topic is still listed alongside
///                            its virtual name in Metadata responses instead of being hidden
///                            (renamed to the virtual name in place) - hidden by default
/// @param valueFormat         optional encoding of record values; when set, content-based filters
///                            see the decoded value instead of the raw string
/// @param transformations     steps applied in order to the record value on fetch; empty (the
///                            default) passes the value through unchanged. Requires `valueFormat`,
///                            and a [ConvertTransformationConfig] may only be the last step. An Avro
///                            output format must name the [AvroFormatConfig#subject] to encode with
/// @param managedBy           the owner of this entry, or `null` when it is unmanaged
public record VirtualTopicConfig(
        String topic,
        @Nullable VirtualTopicFilterConfig filter,
        boolean exposePhysicalTopic,
        @Nullable PayloadFormatConfig valueFormat,
        List<TransformationConfig> transformations,
        ManagedBy managedBy
) {

    public VirtualTopicConfig {
        transformations = transformations == null ? List.of() : List.copyOf(transformations);
        if (!transformations.isEmpty() && valueFormat == null) {
            throw new IllegalArgumentException("transformations require a 'valueFormat' to decode the record value");
        }
        for (int i = 0; i < transformations.size() - 1; i++) {
            if (transformations.get(i) instanceof ConvertTransformationConfig) {
                throw new IllegalArgumentException("a 'convert' transformation must be the last step");
            }
        }
        if (!transformations.isEmpty() && outputFormat(valueFormat, transformations) instanceof AvroFormatConfig avro
                && avro.subject() == null) {
            throw new IllegalArgumentException("encoding transformed values as Avro requires a 'subject'");
        }
    }

    /// The format transformed values are encoded in: the trailing `convert` step's, else `valueFormat`.
    private static PayloadFormatConfig outputFormat(PayloadFormatConfig valueFormat, List<TransformationConfig> transformations) {
        return transformations.getLast() instanceof ConvertTransformationConfig convert ? convert.format() : valueFormat;
    }

    public VirtualTopicConfig(String topic) {
        this(topic, null, false, null);
    }

    public VirtualTopicConfig(String topic, VirtualTopicFilterConfig filter) {
        this(topic, filter, false);
    }

    public VirtualTopicConfig(String topic, VirtualTopicFilterConfig filter, boolean exposePhysicalTopic) {
        this(topic, filter, exposePhysicalTopic, null);
    }

    public VirtualTopicConfig(String topic, VirtualTopicFilterConfig filter, boolean exposePhysicalTopic, PayloadFormatConfig valueFormat) {
        this(topic, filter, exposePhysicalTopic, valueFormat, null);
    }

    public VirtualTopicConfig(
            String topic,
            VirtualTopicFilterConfig filter,
            boolean exposePhysicalTopic,
            PayloadFormatConfig valueFormat,
            ManagedBy managedBy
    ) {
        this(topic, filter, exposePhysicalTopic, valueFormat, List.of(), managedBy);
    }

    /// Returns a copy owned by `managedBy` (`null` for unmanaged).
    public VirtualTopicConfig withManagedBy(ManagedBy managedBy) {
        return new VirtualTopicConfig(topic, filter, exposePhysicalTopic, valueFormat, transformations, managedBy);
    }
}

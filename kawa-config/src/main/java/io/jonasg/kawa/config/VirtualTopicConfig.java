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
/// @param transformations     steps applied in order to the record value and headers on fetch; empty (the
///                            default) passes the value through unchanged. Requires `valueFormat`,
///                            and only header steps may follow a [ConvertTransformationConfig]. An Avro
///                            output format must name the [AvroFormatConfig#subject] to encode with,
///                            an XML one its [XmlFormatConfig#root]
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
        int convert = -1;
        for (int i = 0; i < transformations.size(); i++) {
            TransformationConfig step = transformations.get(i);
            if (convert >= 0 && !isHeaderStep(step)) {
                throw new IllegalArgumentException("transformations[" + convert + "] (convert): only header steps may follow it");
            }
            if (step instanceof ConvertTransformationConfig) {
                convert = i;
            }
        }
        if (!transformations.isEmpty()) {
            switch (outputFormat(valueFormat, transformations)) {
                case AvroFormatConfig avro when avro.subject() == null ->
                        throw new IllegalArgumentException("encoding transformed values as Avro requires a 'subject'");
                case XmlFormatConfig xml when xml.root() == null ->
                        throw new IllegalArgumentException("encoding transformed values as XML requires a 'root'");
                default -> {
                }
            }
        }
    }

    /// The format transformed values are encoded in: the `convert` step's, else `valueFormat`;
    /// `null` when there are no transformations.
    public @Nullable PayloadFormatConfig outputFormat() {
        return transformations.isEmpty() ? null : outputFormat(valueFormat, transformations);
    }

    private static boolean isHeaderStep(TransformationConfig step) {
        return step instanceof DropHeadersTransformationConfig || step instanceof SetHeaderTransformationConfig;
    }

    private static PayloadFormatConfig outputFormat(PayloadFormatConfig valueFormat, List<TransformationConfig> transformations) {
        return transformations.stream()
                .filter(ConvertTransformationConfig.class::isInstance)
                .map(step -> ((ConvertTransformationConfig) step).format())
                .findFirst()
                .orElse(valueFormat);
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

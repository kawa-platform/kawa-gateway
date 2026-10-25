package io.jonasg.kawa.config;

/// Virtual topic configuration.
///
/// @param topic               physical topic name
/// @param filter              optional server-side consume filter configuration
/// @param exposePhysicalTopic when `true`, the physical topic is still listed alongside
///                            its virtual name in Metadata responses instead of being hidden
///                            (renamed to the virtual name in place) - hidden by default
/// @param valueFormat         optional encoding of record values; when set, content-based filters
///                            see the decoded value instead of the raw string
/// @param managedBy           the owner of this entry, or `null` when it is unmanaged
public record VirtualTopicConfig(
        String topic,
        VirtualTopicFilterConfig filter,
        boolean exposePhysicalTopic,
        PayloadFormatConfig valueFormat,
        ManagedBy managedBy
) {

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

    /// Returns a copy owned by `managedBy` (`null` for unmanaged).
    public VirtualTopicConfig withManagedBy(ManagedBy managedBy) {
        return new VirtualTopicConfig(topic, filter, exposePhysicalTopic, valueFormat, managedBy);
    }
}

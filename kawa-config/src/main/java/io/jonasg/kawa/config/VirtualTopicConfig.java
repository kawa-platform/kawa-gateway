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
public record VirtualTopicConfig(
        String topic,
        VirtualTopicFilterConfig filter,
        boolean exposePhysicalTopic,
        PayloadFormatConfig valueFormat
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
}

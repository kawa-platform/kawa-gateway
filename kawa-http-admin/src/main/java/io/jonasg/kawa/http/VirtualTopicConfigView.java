package io.jonasg.kawa.http;

import io.jonasg.kawa.config.PayloadFormatConfig;
import io.jonasg.kawa.config.VirtualTopicFilterConfig;

/// A virtual topic in the admin `/topics` response.
///
/// @param topic               physical topic name
/// @param filter              optional server-side consume filter configuration
/// @param exposePhysicalTopic when `true`, the physical topic is still listed alongside its
///                            virtual name in Metadata responses instead of being hidden
/// @param valueFormat         optional encoding of record values; when set, content-based filters
///                            see the decoded value instead of the raw string
public record VirtualTopicConfigView(
        String topic,
        VirtualTopicFilterConfig filter,
        boolean exposePhysicalTopic,
        PayloadFormatConfig valueFormat
) {
}

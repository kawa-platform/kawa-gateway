package io.jonasg.kawa.http;

import io.jonasg.kawa.config.PayloadFormatConfig;

/// A single topic entry in the admin `/topics` response.
///
/// @param type              either `"virtual"` (virtual topic with an alias) or `"physical"` (raw broker topic)
/// @param name              the display name — virtual alias for virtual topics, physical name otherwise
/// @param partitions        number of partitions (from the live metadata cache)
/// @param replicationFactor number of replicas per partition (from the live metadata cache)
/// @param filter            structured consume-filter, or `null` if none
/// @param physicalTopic     the broker-side topic name, only present when `type` is `"virtual"`
/// @param valueFormat       encoding of record values, or `null` if none (raw strings)
public record TopicView(
        String type,
        String name,
        int partitions,
        int replicationFactor,
        TopicFilterView filter,
        String physicalTopic,
        PayloadFormatConfig valueFormat) {
}

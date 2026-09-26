package io.jonasg.kawa.http;

import java.util.Map;

/// The created topic echoed in the admin `POST /topics` response, after the broker
/// accepted it. A `-1` partitions or replication factor means the client left it to
/// the broker default.
///
/// @param name              the topic name
/// @param partitions        requested partitions, or `-1` for the broker default
/// @param replicationFactor requested replication factor, or `-1` for the broker default
/// @param configs           topic configs, keyed by config name
public record TopicSpecView(
        String name,
        int partitions,
        int replicationFactor,
        Map<String, String> configs
) {
}

package io.jonasg.kawa.http;

import org.jspecify.annotations.NullUnmarked;

import java.util.Map;

/// A `POST /governance/dry-run` body: a request to evaluate without creating anything.
/// Validated and converted by [GovernanceConfigMapper].
///
/// @param resourceType `TOPIC`, `GROUP` or `TRANSACTIONAL_ID`
/// @param operation    `CREATE` (default) or `ALTER`; on alter, partitions and replication
///                     factor are unknown and `configs` holds the changed configs only
/// @param virtual      topics only: evaluate as a virtual topic; absent means physical
/// @param resource     the resource's fields: for a physical topic `name`, `partitions`,
///                     `replicationFactor`, `configs`; for a virtual topic `name`,
///                     `physicalTopic`; for a group or transactional id `id`
/// @param principal    the requesting principal
/// @param service      the service the request came through
/// @param rule         optional unsaved rule to evaluate instead of the stored ones; global
///                     exemptions are not consulted then
@NullUnmarked
public record GovernanceDryRunRequest(
        String resourceType,
        String operation,
        Boolean virtual,
        Map<String, Object> resource,
        String principal,
        String service,
        GovernanceRuleRequest rule
) {
}

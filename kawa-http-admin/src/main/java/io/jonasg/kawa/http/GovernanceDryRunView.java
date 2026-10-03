package io.jonasg.kawa.http;

import io.jonasg.kawa.governance.GovernanceTrace;

import java.util.List;

/// The `POST /governance/dry-run` response. The nested traces stay `kawa-governance` records.
///
/// @param allowed    whether the request would pass: globally exempted, or no rule failed
/// @param exemptedBy the global exemption that matched, or `null`
/// @param rules      one trace per rule for the request's resource type and topic kind
public record GovernanceDryRunView(
        boolean allowed,
        String exemptedBy,
        List<GovernanceTrace.RuleTrace> rules
) {
}

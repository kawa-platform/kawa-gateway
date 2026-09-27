package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GovernanceExemptionConfig;
import io.jonasg.kawa.config.GovernanceRuleConfig;

import java.util.Map;

/// The governance section of a `GET` or `PUT /governance` response. Only the top level is a
/// view type: every rule and exemption keeps its `kawa-config` record, because the admin UI
/// round-trips these nested bodies unchanged.
///
/// @param topicRules named rules, each a message plus a CEL expression
/// @param exemptions named exemptions, each a principal regex plus a topic pattern regex
public record GovernanceConfigView(
        Map<String, GovernanceRuleConfig> topicRules,
        Map<String, GovernanceExemptionConfig> exemptions
) {
}

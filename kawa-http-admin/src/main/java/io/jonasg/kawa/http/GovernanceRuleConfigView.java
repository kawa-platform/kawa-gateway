package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GovernanceRuleConfig;

/// A single governance rule in an admin `/governance/rules` response. The nested selector and
/// expression stay `kawa-config` records: only the top-level body is a transport type.
///
/// @param name         unique, human-readable name of the rule
/// @param errorMessage message shown when the rule rejects a resource
/// @param description  longer explanation of what the rule enforces
/// @param selector     which Kafka resources the rule applies to
/// @param expression   expression that must evaluate to `true` for the resource to be compliant
public record GovernanceRuleConfigView(
        String name,
        String errorMessage,
        String description,
        GovernanceRuleConfig.Selector selector,
        GovernanceRuleConfig.Expression expression) {
}

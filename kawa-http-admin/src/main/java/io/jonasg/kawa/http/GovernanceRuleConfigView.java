package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GovernanceRuleConfig;

import java.util.List;

/// A single governance rule in an admin `/governance/rules` response. The nested selector,
/// sub-rules and exemptions stay `kawa-config` records: only the top-level body is a transport type.
///
/// @param name         unique, human-readable name of the rule
/// @param errorMessage message shown when the rule rejects a resource
/// @param description  longer explanation of what the rule enforces
/// @param selector     which Kafka resources the rule applies to
/// @param match        how [subRules] combine: `ALL` or `ANY`
/// @param subRules     the checks and groups making up the rule
/// @param exemptions   named cases the rule does not apply to; empty when there are none
public record GovernanceRuleConfigView(
        String name,
        String errorMessage,
        String description,
        GovernanceRuleConfig.Selector selector,
        GovernanceRuleConfig.Match match,
        List<GovernanceRuleConfig.SubRule> subRules,
        List<GovernanceRuleConfig.Exemption> exemptions) {
}

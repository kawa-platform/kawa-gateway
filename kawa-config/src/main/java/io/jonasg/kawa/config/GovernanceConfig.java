package io.jonasg.kawa.config;

import java.util.HashMap;
import java.util.Map;

/// Topic governance configuration: named rules that new topics must satisfy. Each rule carries
/// its own exemptions, see [GovernanceRuleConfig#exemptions].
///
/// @param rules named rules
public record GovernanceConfig(
        Map<String, GovernanceRuleConfig> rules
) {

    public GovernanceConfig {
        rules = rules == null ? Map.of() : Map.copyOf(rules);
    }

    /// Returns a new [GovernanceConfig] with the given rule added or replaced.
    public GovernanceConfig upsertRule(String name, GovernanceRuleConfig rule) {
        var newRules = new HashMap<>(rules);
        newRules.put(name, rule);
        return new GovernanceConfig(newRules);
    }

    /// Returns a new [GovernanceConfig] with the given rule removed.
    public GovernanceConfig removeRule(String name) {
        var newRules = new HashMap<>(rules);
        newRules.remove(name);
        return new GovernanceConfig(newRules);
    }
}

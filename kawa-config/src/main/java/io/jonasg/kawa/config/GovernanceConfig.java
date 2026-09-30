package io.jonasg.kawa.config;

import java.util.HashMap;
import java.util.Map;

/// Topic governance configuration: named CEL rules that new topics must satisfy, and named
/// exemptions that skip evaluation for matching principal + topic pairs.
///
/// @param rules named rules, each a message plus a CEL expression
/// @param exemptions named exemptions, each a principal regex plus a topic pattern regex
public record GovernanceConfig(
        Map<String, GovernanceRuleConfig> rules,
        Map<String, GovernanceExemptionConfig> exemptions
) {

    public GovernanceConfig {
        rules = rules == null ? Map.of() : Map.copyOf(rules);
        exemptions = exemptions == null ? Map.of() : Map.copyOf(exemptions);
    }

    /// Returns a new [GovernanceConfig] with the given rule added or replaced.
    public GovernanceConfig upsertRule(String name, GovernanceRuleConfig rule) {
        var newRules = new HashMap<>(rules);
        newRules.put(name, rule);
        return new GovernanceConfig(newRules, exemptions);
    }

    /// Returns a new [GovernanceConfig] with the given rule removed.
    public GovernanceConfig removeRule(String name) {
        var newRules = new HashMap<>(rules);
        newRules.remove(name);
        return new GovernanceConfig(newRules, exemptions);
    }

    /// Returns a new [GovernanceConfig] with the given exemption added or replaced.
    public GovernanceConfig upsertExemption(String name, GovernanceExemptionConfig exemption) {
        var newExemptions = new HashMap<>(exemptions);
        newExemptions.put(name, exemption);
        return new GovernanceConfig(rules, newExemptions);
    }

    /// Returns a new [GovernanceConfig] with the given exemption removed.
    public GovernanceConfig removeExemption(String name) {
        var newExemptions = new HashMap<>(exemptions);
        newExemptions.remove(name);
        return new GovernanceConfig(rules, newExemptions);
    }
}

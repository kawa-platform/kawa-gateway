package io.jonasg.kawa.config;

import java.util.HashMap;
import java.util.Map;

/// Governance configuration: named rules that resources must satisfy, global exemptions, and
/// variables. Each rule carries its own exemptions too, see [GovernanceRuleConfig#exemptions]; a
/// global exemption skips every rule when its expression is `true` for a request. Variables are
/// typed constants every expression can read.
///
/// @param rules      named rules
/// @param exemptions named global exemptions; never `null`
/// @param variables  named variables; never `null`
public record GovernanceConfig(
        Map<String, GovernanceRuleConfig> rules,
        Map<String, GovernanceRuleConfig.Exemption> exemptions,
        Map<String, GovernanceVariableConfig> variables
) {

    /// Rules only, no global exemptions or variables.
    public GovernanceConfig(Map<String, GovernanceRuleConfig> rules) {
        this(rules, null, null);
    }

    /// Rules and global exemptions, no variables.
    public GovernanceConfig(Map<String, GovernanceRuleConfig> rules, Map<String, GovernanceRuleConfig.Exemption> exemptions) {
        this(rules, exemptions, null);
    }

    public GovernanceConfig {
        rules = rules == null ? Map.of() : Map.copyOf(rules);
        exemptions = exemptions == null ? Map.of() : Map.copyOf(exemptions);
        variables = variables == null ? Map.of() : Map.copyOf(variables);
    }

    /// Returns a new [GovernanceConfig] with the given rule added or replaced.
    public GovernanceConfig upsertRule(String name, GovernanceRuleConfig rule) {
        var newRules = new HashMap<>(rules);
        newRules.put(name, rule);
        return new GovernanceConfig(newRules, exemptions, variables);
    }

    /// Returns a new [GovernanceConfig] with the given rule removed.
    public GovernanceConfig removeRule(String name) {
        var newRules = new HashMap<>(rules);
        newRules.remove(name);
        return new GovernanceConfig(newRules, exemptions, variables);
    }

    /// Returns a new [GovernanceConfig] with the given global exemption added or replaced.
    public GovernanceConfig upsertExemption(String name, GovernanceRuleConfig.Exemption exemption) {
        var newExemptions = new HashMap<>(exemptions);
        newExemptions.put(name, exemption);
        return new GovernanceConfig(rules, newExemptions, variables);
    }

    /// Returns a new [GovernanceConfig] with the given global exemption removed.
    public GovernanceConfig removeExemption(String name) {
        var newExemptions = new HashMap<>(exemptions);
        newExemptions.remove(name);
        return new GovernanceConfig(rules, newExemptions, variables);
    }

    /// Returns a new [GovernanceConfig] with the given variable added or replaced.
    public GovernanceConfig upsertVariable(GovernanceVariableConfig variable) {
        var newVariables = new HashMap<>(variables);
        newVariables.put(variable.name(), variable);
        return new GovernanceConfig(rules, exemptions, newVariables);
    }

    /// Returns a new [GovernanceConfig] with the given variable removed.
    public GovernanceConfig removeVariable(String name) {
        var newVariables = new HashMap<>(variables);
        newVariables.remove(name);
        return new GovernanceConfig(rules, exemptions, newVariables);
    }
}

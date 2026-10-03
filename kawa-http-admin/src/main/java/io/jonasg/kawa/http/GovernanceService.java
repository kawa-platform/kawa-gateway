package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfigRepository;
import io.jonasg.kawa.config.GovernanceConfig;
import io.jonasg.kawa.config.GovernanceRuleConfig;
import io.jonasg.kawa.config.GovernanceVariableConfig;
import io.jonasg.kawa.governance.GovernancePolicy;

/// Owns reading and writing the governance config snapshot.
final class GovernanceService {

    private final GatewayConfigRepository repository;
    private final ConsistencyAwareUpdater updater;

    GovernanceService(GatewayConfigRepository repository) {
        this.repository = repository;
        this.updater = new ConsistencyAwareUpdater(repository);
    }

    GovernanceConfig get() {
        return repository.getActiveConfigOrEmpty().governance();
    }

    /// The rule stored under `name`.
    ///
    /// @throws NotFoundException if no rule has that name
    GovernanceRuleConfig getRule(String name) {
        var rule = get().rules().get(name);
        if (rule == null) {
            throw new NotFoundException("governance rule '" + name + "' not found");
        }
        return rule;
    }

    GovernanceRuleConfig upsertRule(
            GovernanceRuleConfig governanceRuleConfig,
            Consistency consistency
    ) {
        updater.update(consistency, gatewayCfg -> gatewayCfg.upsertGovernanceRule(governanceRuleConfig));
        return governanceRuleConfig;
    }

    /// Removes the rule stored under `name`.
    ///
    /// @throws NotFoundException if no rule has that name
    void deleteRule(String name, Consistency consistency) {
        getRule(name);
        updater.update(consistency, gatewayCfg -> gatewayCfg.removeGovernanceRule(name));
    }

    /// The global exemption stored under `name`.
    ///
    /// @throws NotFoundException if no global exemption has that name
    GovernanceRuleConfig.Exemption getExemption(String name) {
        var exemption = get().exemptions().get(name);
        if (exemption == null) {
            throw new NotFoundException("governance exemption '" + name + "' not found");
        }
        return exemption;
    }

    GovernanceRuleConfig.Exemption upsertExemption(GovernanceRuleConfig.Exemption exemption, Consistency consistency) {
        updater.update(consistency, gatewayCfg -> gatewayCfg.upsertGovernanceExemption(exemption));
        return exemption;
    }

    /// The variable stored under `name`.
    ///
    /// @throws NotFoundException if no variable has that name
    GovernanceVariableConfig getVariable(String name) {
        var variable = get().variables().get(name);
        if (variable == null) {
            throw new NotFoundException("governance variable '" + name + "' not found");
        }
        return variable;
    }

    /// Adds or replaces a variable. The whole governance section is compiled with the change
    /// first, so a type change that breaks a rule reading the variable is refused.
    ///
    /// @throws IllegalArgumentException if a rule or exemption no longer compiles
    GovernanceVariableConfig upsertVariable(GovernanceVariableConfig variable, Consistency consistency) {
        GovernancePolicy.validate(get().upsertVariable(variable));
        updater.update(consistency, gatewayCfg -> gatewayCfg.updateGovernance(gatewayCfg.governance().upsertVariable(variable)));
        return variable;
    }

    /// Removes the variable stored under `name`.
    ///
    /// @throws NotFoundException       if no variable has that name
    /// @throws IllegalArgumentException if a rule or exemption still reads it
    void deleteVariable(String name, Consistency consistency) {
        getVariable(name);
        GovernancePolicy.validate(get().removeVariable(name));
        updater.update(consistency, gatewayCfg -> gatewayCfg.updateGovernance(gatewayCfg.governance().removeVariable(name)));
    }

    /// Removes the global exemption stored under `name`.
    ///
    /// @throws NotFoundException if no global exemption has that name
    void deleteExemption(String name, Consistency consistency) {
        getExemption(name);
        updater.update(consistency, gatewayCfg -> gatewayCfg.removeGovernanceExemption(name));
    }
}

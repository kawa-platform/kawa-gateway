package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfigRepository;
import io.jonasg.kawa.config.GovernanceConfig;
import io.jonasg.kawa.config.GovernanceRuleConfig;

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

    GovernanceRuleConfig upsertRule(
            GovernanceRuleConfig governanceRuleConfig,
            Consistency consistency
    ) {
        updater.update(consistency, gatewayCfg -> gatewayCfg.upsertGovernanceRule(governanceRuleConfig));
        return governanceRuleConfig;
    }
}

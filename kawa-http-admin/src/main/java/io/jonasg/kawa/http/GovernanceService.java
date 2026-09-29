package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfigRepository;
import io.jonasg.kawa.config.GovernanceConfig;

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

    GovernanceConfig updateGovernance(GovernanceConfigRequest request, Consistency consistency) {
        GovernanceConfigMapper mapper = new GovernanceConfigMapper();
        GovernanceConfig value = mapper.toConfig(request);
        updater.update(consistency, config -> config.updateGovernance(value));
        return value;
    }
}

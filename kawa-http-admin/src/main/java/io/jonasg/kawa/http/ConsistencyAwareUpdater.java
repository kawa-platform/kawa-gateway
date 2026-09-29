package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.GatewayConfigRepository;

import java.util.function.UnaryOperator;

/// Applies mutations to the gateway config snapshot with the requested consistency.
final class ConsistencyAwareUpdater {

    private final GatewayConfigRepository repository;

    ConsistencyAwareUpdater(GatewayConfigRepository repository) {
        this.repository = repository;
    }

    void update(Consistency consistency, UnaryOperator<GatewayConfig> mutation) {
        if (consistency == Consistency.APPLIED) {
            repository.updateAndWaitUntilApplied(mutation);
            return;
        }
        repository.update(mutation);
    }
}

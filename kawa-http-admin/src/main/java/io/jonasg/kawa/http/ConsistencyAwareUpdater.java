package io.jonasg.kawa.http;

import io.jonasg.kawa.config.DynamicConfig;
import io.jonasg.kawa.config.DynamicConfigRepository;

import java.util.function.UnaryOperator;

/// Applies mutations to the gateway config snapshot with the requested consistency.
final class ConsistencyAwareUpdater {

    private final DynamicConfigRepository repository;

    ConsistencyAwareUpdater(DynamicConfigRepository repository) {
        this.repository = repository;
    }

    void update(Consistency consistency, UnaryOperator<DynamicConfig> mutation) {
        if (consistency == Consistency.APPLIED) {
            repository.updateAndWaitUntilApplied(mutation);
            return;
        }
        repository.update(mutation);
    }
}

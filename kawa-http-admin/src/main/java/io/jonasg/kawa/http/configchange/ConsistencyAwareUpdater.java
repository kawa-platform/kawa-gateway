package io.jonasg.kawa.http.configchange;

import io.jonasg.kawa.config.DynamicConfig;
import io.jonasg.kawa.config.DynamicConfigRepository;

import java.util.function.UnaryOperator;

/// Applies mutations to the gateway config snapshot with the requested consistency.
public final class ConsistencyAwareUpdater {

    private final DynamicConfigRepository repository;

    public ConsistencyAwareUpdater(DynamicConfigRepository repository) {
        this.repository = repository;
    }

    public void update(Consistency consistency, UnaryOperator<DynamicConfig> mutation) {
        if (consistency == Consistency.APPLIED) {
            repository.updateAndWaitUntilApplied(mutation);
            return;
        }
        repository.update(mutation);
    }
}

package io.jonasg.kawa.http;

import io.jonasg.kawa.config.DynamicConfig;
import io.jonasg.kawa.config.DynamicConfigRepository;

import java.util.function.UnaryOperator;

/// In-memory [DynamicConfigRepository] for handler tests: [update] replaces the current
/// snapshot synchronously, mirroring what the config-topic consumer does asynchronously.
final class FakeDynamicConfigRepository implements DynamicConfigRepository {

    private DynamicConfig current;

    FakeDynamicConfigRepository(DynamicConfig initial) {
        this.current = initial;
    }

    @Override
    public DynamicConfig getActiveConfig() {
        return current;
    }

    @Override
    public void update(UnaryOperator<DynamicConfig> mutation) {
        this.current = mutation.apply(getActiveConfigOrEmpty());
    }

    @Override
    public void updateAndWaitUntilApplied(UnaryOperator<DynamicConfig> mutation) {
        this.current = mutation.apply(getActiveConfigOrEmpty());
    }

    @Override
    public void close() {
        // no resources
    }
}

package io.jonasg.kawa.mcp;

import io.jonasg.kawa.config.DynamicConfig;
import io.jonasg.kawa.config.DynamicConfigRepository;

import java.util.function.UnaryOperator;

/// Holds the config in memory for the MCP tests; a write applies at once.
final class InMemoryConfigRepository implements DynamicConfigRepository {

    private DynamicConfig current;

    InMemoryConfigRepository(DynamicConfig initial) {
        this.current = initial;
    }

    @Override
    public DynamicConfig getActiveConfig() {
        return current;
    }

    @Override
    public synchronized void update(UnaryOperator<DynamicConfig> mutation) {
        current = mutation.apply(current);
    }

    @Override
    public void updateAndWaitUntilApplied(UnaryOperator<DynamicConfig> mutation) {
        update(mutation);
    }

    @Override
    public void close() {
    }
}

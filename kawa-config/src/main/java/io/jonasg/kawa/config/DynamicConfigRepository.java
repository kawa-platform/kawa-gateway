package io.jonasg.kawa.config;

import java.util.function.UnaryOperator;

/// Read/write access to the dynamic gateway config. [getActiveConfig] returns the most recently
/// persisted snapshot (falling back to the last applied one before any write); [update]
/// applies a mutation to the active config and persists the result, which the consumer applies
/// asynchronously through the normal flow.
public interface DynamicConfigRepository extends AutoCloseable {

    /// The most recent config snapshot, or `null` before any snapshot has been persisted or
    /// applied (an empty config topic on first boot).
    DynamicConfig getActiveConfig();

    /// The active config, or an empty config before the first snapshot has been persisted or
    /// applied. Convenience for callers that need a non-null base (e.g. the admin API's read
    /// paths).
    default DynamicConfig getActiveConfigOrEmpty() {
        DynamicConfig active = getActiveConfig();
        return active != null ? active : DynamicConfig.empty();
    }

    /// Applies mutation to the active config (or an empty config before the first snapshot)
    /// and persists the result. The read-modify-write base is resolved by the repository, so
    /// callers never see the null-before-first-snapshot case.
    void update(UnaryOperator<DynamicConfig> mutation);

    /// Applies mutation to the active config (or an empty config before the first snapshot),
    /// persists the result, and only returns when the write mode's consistency target has been
    /// reached. Repositories that only observe persistence may alias this to [update].
    void updateAndWaitUntilApplied(UnaryOperator<DynamicConfig> mutation);

    /// Releases the repository's resources (e.g. the config-topic producer).
    @Override
    void close();
}

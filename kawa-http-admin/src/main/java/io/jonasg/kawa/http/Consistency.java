package io.jonasg.kawa.http;

import org.jspecify.annotations.Nullable;

/// Consistency mode for config snapshot mutations triggered through the admin HTTP API.
enum Consistency {

    /// Persist the mutation asynchronously; do not wait for it to be applied.
    PERSISTED,

    /// Persist the mutation and block until it has been applied cluster-wide.
    APPLIED;

    /// Parses the `consistency` query parameter value into a [Consistency].
    /// Accepts `null` and `"persisted"` as [PERSISTED], and `"applied"` as [APPLIED].
    static Consistency fromQueryParam(@Nullable String value) {
        if (value == null || value.equals("persisted")) {
            return PERSISTED;
        }
        if (value.equals("applied")) {
            return APPLIED;
        }
        throw new IllegalArgumentException(
                "invalid consistency '" + value + "' (expected 'persisted' or 'applied')");
    }
}

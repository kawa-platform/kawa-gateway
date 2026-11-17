package io.jonasg.kawa.core.cluster;

import java.util.concurrent.CompletionStage;

/// Fetches the cluster metadata now instead of waiting for the next scheduled refresh, for callers
/// that must see their own broker change in the [MetadataCache] (such as the admin API right after
/// it created a topic).
@FunctionalInterface
public interface MetadataRefresher {

    /// Requests the cluster metadata once; completes once the [MetadataCache] holds the result, or
    /// exceptionally when the metadata could not be requested.
    CompletionStage<Void> refresh();
}

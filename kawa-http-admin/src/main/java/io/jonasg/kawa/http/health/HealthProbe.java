package io.jonasg.kawa.http.health;

import java.time.Duration;
import java.util.Optional;

/// The gateway state the readiness probe reports on. Read on every request, so readiness follows
/// the live state: it recovers without a restart once the state does.
public interface HealthProbe {

    /// Whether the Kafka client listener is bound.
    boolean listenerBound();

    /// Whether the dynamic config has been loaded from the config topic.
    boolean configLoaded();

    /// Time since the cluster view was last refreshed from the brokers, or empty if it never was.
    Optional<Duration> timeSinceClusterRefresh();
}

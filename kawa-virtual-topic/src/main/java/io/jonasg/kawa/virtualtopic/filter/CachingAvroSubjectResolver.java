package io.jonasg.kawa.virtualtopic.filter;

import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/// Caches the schemas another [AvroSubjectResolver] resolves, so encoding a record does not ask
/// the registry.
///
/// An explicit subject version always names the same schema, so it is cached for good. The
/// latest version changes when a new one is registered, so it is looked up again once it is
/// older than [#DEFAULT_LATEST_TTL].
///
/// A failed lookup is remembered for [#DEFAULT_FAILURE_TTL], the same way as in
/// [CachingAvroSchemaResolver]: within it, the lookup fails again without asking the registry.
///
/// When looking up the latest version again fails with a [SchemaRegistryUnavailableException],
/// the latest version resolved before is kept until the registry answers again, so an outage does
/// not stop encoding. Any other failure - the subject was deleted, say - fails even then, and an
/// outage with nothing cached yet fails with the [SchemaRegistryUnavailableException].
public final class CachingAvroSubjectResolver implements AvroSubjectResolver {

    public static final Duration DEFAULT_LATEST_TTL = Duration.ofMinutes(5);
    public static final Duration DEFAULT_FAILURE_TTL = CachingAvroSchemaResolver.DEFAULT_FAILURE_TTL;

    private final AvroSubjectResolver delegate;
    private final long latestTtlNanos;
    private final long failureTtlNanos;
    private final LongSupplier nanoClock;
    private final Map<Key, Resolved> schemas = new ConcurrentHashMap<>();
    private final Map<Key, Failure> failures = new ConcurrentHashMap<>();

    public CachingAvroSubjectResolver(AvroSubjectResolver delegate) {
        this(delegate, DEFAULT_LATEST_TTL, DEFAULT_FAILURE_TTL, System::nanoTime);
    }

    CachingAvroSubjectResolver(AvroSubjectResolver delegate, Duration latestTtl, Duration failureTtl, LongSupplier nanoClock) {
        this.delegate = delegate;
        this.latestTtlNanos = latestTtl.toNanos();
        this.failureTtlNanos = failureTtl.toNanos();
        this.nanoClock = nanoClock;
    }

    @Override
    public RegisteredAvroSchema resolve(String subject, @Nullable Integer version) {
        var key = new Key(subject, version);
        long now = nanoClock.getAsLong();
        Resolved cached = schemas.get(key);
        if (cached != null && (version != null || now - cached.resolvedAtNanos() < latestTtlNanos)) {
            return cached.schema();
        }
        Failure failure = failures.get(key);
        if (failure != null && now - failure.failedAtNanos() < failureTtlNanos) {
            if (cached != null && failure.error() instanceof SchemaRegistryUnavailableException) {
                return cached.schema();
            }
            throw failure.recalled();
        }
        // Resolved outside the map, as in CachingAvroSchemaResolver, so no map lock is held for
        // the duration of a registry call.
        RegisteredAvroSchema resolved;
        try {
            resolved = delegate.resolve(subject, version);
        } catch (RuntimeException e) {
            failures.put(key, new Failure(e, now));
            if (cached != null && e instanceof SchemaRegistryUnavailableException) {
                // The registry is down: keep encoding with the last latest version it gave.
                return cached.schema();
            }
            throw e;
        }
        failures.remove(key);
        schemas.put(key, new Resolved(resolved, now));
        return resolved;
    }

    private record Key(String subject, @Nullable Integer version) {
    }

    private record Resolved(RegisteredAvroSchema schema, long resolvedAtNanos) {
    }

    private record Failure(RuntimeException error, long failedAtNanos) {

        /// The remembered failure, thrown again: a registry outage stays a
        /// [SchemaRegistryUnavailableException], so it is still not taken for a record error.
        RuntimeException recalled() {
            String message = error.getMessage() + " (failed recently, not retried yet)";
            return error instanceof SchemaRegistryUnavailableException
                    ? new SchemaRegistryUnavailableException(message, error)
                    : new IllegalStateException(message, error);
        }
    }
}

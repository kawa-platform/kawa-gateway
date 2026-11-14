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
            throw new IllegalStateException(failure.error().getMessage() + " (failed recently, not retried yet)", failure.error());
        }
        // Resolved outside the map, as in CachingAvroSchemaResolver, so no map lock is held for
        // the duration of a registry call.
        RegisteredAvroSchema resolved;
        try {
            resolved = delegate.resolve(subject, version);
        } catch (RuntimeException e) {
            failures.put(key, new Failure(e, now));
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
    }
}

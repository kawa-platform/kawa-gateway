package io.jonasg.kawa.virtualtopic.filter;

import org.apache.avro.Schema;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/// Caches the schemas another [AvroSchemaResolver] resolves, so each registry id is looked up
/// once. A registry id always names the same schema, so entries never expire.
///
/// A failed lookup is remembered for a short while ([#DEFAULT_FAILURE_TTL]): within it, the id
/// fails again without asking the registry, so a topic full of records with an unknown id - or a
/// registry that is down - costs one registry call per id per interval instead of one per record.
/// Afterwards the id is looked up again, so a schema registered later is still found.
public final class CachingAvroSchemaResolver implements AvroSchemaResolver {

    public static final Duration DEFAULT_FAILURE_TTL = Duration.ofSeconds(5);

    private final AvroSchemaResolver delegate;
    private final long failureTtlNanos;
    private final LongSupplier nanoClock;
    private final Map<Integer, Schema> schemas = new ConcurrentHashMap<>();
    private final Map<Integer, Failure> failures = new ConcurrentHashMap<>();

    public CachingAvroSchemaResolver(AvroSchemaResolver delegate) {
        this(delegate, DEFAULT_FAILURE_TTL, System::nanoTime);
    }

    CachingAvroSchemaResolver(AvroSchemaResolver delegate, Duration failureTtl, LongSupplier nanoClock) {
        this.delegate = delegate;
        this.failureTtlNanos = failureTtl.toNanos();
        this.nanoClock = nanoClock;
    }

    @Override
    public Schema resolve(int schemaId) {
        Schema cached = schemas.get(schemaId);
        if (cached != null) {
            return cached;
        }
        long now = nanoClock.getAsLong();
        Failure failure = failures.get(schemaId);
        if (failure != null && now - failure.failedAtNanos() < failureTtlNanos) {
            throw new IllegalStateException(failure.error().getMessage() + " (failed recently, not retried yet)", failure.error());
        }
        // Resolved outside the map rather than in computeIfAbsent, which would hold a map lock
        // for the duration of a registry call. Two threads missing on the same id may both
        // resolve it; the first schema stored wins and both return it.
        Schema resolved;
        try {
            resolved = delegate.resolve(schemaId);
        } catch (RuntimeException e) {
            // Drop expired failures here, so ids that never come back do not pile up.
            failures.values().removeIf(old -> now - old.failedAtNanos() >= failureTtlNanos);
            failures.put(schemaId, new Failure(e, now));
            throw e;
        }
        failures.remove(schemaId);
        Schema raced = schemas.putIfAbsent(schemaId, resolved);
        return raced != null ? raced : resolved;
    }

    private record Failure(RuntimeException error, long failedAtNanos) {
    }
}

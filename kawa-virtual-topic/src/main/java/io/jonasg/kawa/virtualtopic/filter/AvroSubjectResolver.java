package io.jonasg.kawa.virtualtopic.filter;

import org.jspecify.annotations.Nullable;

/// Looks up the Avro schema registered under a schema registry subject, to encode values with.
///
/// The latest version of a subject changes when a new version is registered, so only lookups of
/// an explicit version can be cached for good: wrap a resolver that calls the registry in a
/// [CachingAvroSubjectResolver]. Implementations must be thread-safe.
@FunctionalInterface
public interface AvroSubjectResolver {

    /// @param version the subject version, `null` for the latest
    /// @throws RuntimeException if the subject or version does not exist or the lookup fails
    RegisteredAvroSchema resolve(String subject, @Nullable Integer version);
}

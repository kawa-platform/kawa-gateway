package io.jonasg.kawa.virtualtopic.filter;

import org.apache.avro.Schema;

/// Looks up the Avro writer schema registered under a schema registry id.
///
/// A registry id always names the same schema, so lookups can be cached for good: wrap a
/// resolver that calls the registry in a [CachingAvroSchemaResolver]. Implementations must be
/// thread-safe.
@FunctionalInterface
public interface AvroSchemaResolver {

    /// @throws RuntimeException if no schema is registered under `schemaId` or the lookup fails
    Schema resolve(int schemaId);
}

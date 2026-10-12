package io.jonasg.kawa.virtualtopic.filter;

import org.jspecify.annotations.Nullable;

/// Finds the schema registry of the cluster that holds a physical topic, so a virtual topic's
/// record values are decoded with the schemas of the cluster its records actually live on.
@FunctionalInterface
public interface SchemaRegistries {

    /// No cluster has a schema registry.
    SchemaRegistries NONE = physicalTopic -> null;

    /// The Avro schemas of the schema registry of the cluster holding `physicalTopic`, or `null`
    /// when that cluster has no schema registry. Returns the same instance for every topic of a
    /// cluster, so the topics share its schema cache.
    @Nullable AvroSchemaResolver avroSchemasFor(String physicalTopic);
}

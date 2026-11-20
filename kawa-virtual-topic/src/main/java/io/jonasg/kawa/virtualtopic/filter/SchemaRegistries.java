package io.jonasg.kawa.virtualtopic.filter;

import org.jspecify.annotations.Nullable;

/// Finds the schema registry of the cluster that holds a physical topic, so a virtual topic's
/// record values are decoded, and its transformed values encoded, with the schemas of the cluster
/// its records actually live on.
public interface SchemaRegistries {

    /// No cluster has a schema registry.
    SchemaRegistries NONE = of(null, null);

    /// The Avro schemas of the schema registry of the cluster holding `physicalTopic`, to decode
    /// with, or `null` when that cluster has no schema registry. Returns the same instance for
    /// every topic of a cluster, so the topics share its schema cache.
    @Nullable AvroSchemaResolver avroSchemasFor(String physicalTopic);

    /// The Avro subjects of the schema registry of the cluster holding `physicalTopic`, to encode
    /// transformed values with, or `null` when that cluster has no schema registry. Returns the
    /// same instance for every topic of a cluster, so the topics share its subject cache.
    @Nullable AvroSubjectResolver avroSubjectsFor(String physicalTopic);

    /// One schema registry for every topic, or none when both are `null`.
    static SchemaRegistries of(@Nullable AvroSchemaResolver avroSchemas, @Nullable AvroSubjectResolver avroSubjects) {
        return new SchemaRegistries() {
            @Override
            public @Nullable AvroSchemaResolver avroSchemasFor(String physicalTopic) {
                return avroSchemas;
            }

            @Override
            public @Nullable AvroSubjectResolver avroSubjectsFor(String physicalTopic) {
                return avroSubjects;
            }
        };
    }
}

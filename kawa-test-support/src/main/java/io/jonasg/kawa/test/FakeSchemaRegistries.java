package io.jonasg.kawa.test;

import io.jonasg.kawa.virtualtopic.filter.SchemaRegistries;

/// [SchemaRegistries] for admin API tests, which only ask whether a topic's cluster has a schema
/// registry, never for a schema.
public final class FakeSchemaRegistries {

    /// Every topic's cluster has a schema registry; looking a schema up in it fails.
    public static final SchemaRegistries PRESENT = SchemaRegistries.of(
            id -> {
                throw new IllegalStateException("no schema id " + id + " in this test");
            },
            (subject, version) -> {
                throw new IllegalStateException("no subject '" + subject + "' in this test");
            });

    private FakeSchemaRegistries() {
    }
}

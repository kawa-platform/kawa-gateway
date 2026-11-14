package io.jonasg.kawa.virtualtopic.filter;

import org.apache.avro.Schema;

/// An Avro schema together with the schema registry id it is registered under.
///
/// @param id     the registry id, written into the wire format ahead of the Avro binary
/// @param schema the parsed schema
public record RegisteredAvroSchema(int id, Schema schema) {
}

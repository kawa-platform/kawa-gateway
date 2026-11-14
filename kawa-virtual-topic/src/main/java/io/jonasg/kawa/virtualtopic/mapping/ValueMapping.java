package io.jonasg.kawa.virtualtopic.mapping;

import org.apache.kafka.common.record.internal.Record;

/// Maps a decoded record value (a value tree, see
/// [io.jonasg.kawa.virtualtopic.filter.PayloadDecoder]) to the value a virtual topic exposes.
///
/// Implementations never modify `value`: decoded trees can be read-only views, so a mapping
/// returns a new tree that shares every part it did not change. They are thread-safe.
@FunctionalInterface
public interface ValueMapping {

    /// Passes the value through unchanged.
    ValueMapping IDENTITY = (value, record) -> value;

    /// @param record the record the value belongs to, for its `key`, `headers` and `timestamp`
    /// @throws PayloadMappingException if this record's value cannot be mapped
    Object apply(Object value, Record record);
}

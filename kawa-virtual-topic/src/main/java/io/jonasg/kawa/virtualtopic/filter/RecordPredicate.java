package io.jonasg.kawa.virtualtopic.filter;

import org.apache.kafka.common.record.internal.Record;

/// Decides whether a fetched record passes a virtual topic's consume filter.
public interface RecordPredicate {

    /// @param value the record's value, decoded on demand: a predicate that does not read the value
    ///              never calls [DecodedValue#get()], so the value is never decoded for it
    /// @throws PayloadDecodeException when the predicate reads a value that does not decode
    boolean test(Record record, DecodedValue value);
}

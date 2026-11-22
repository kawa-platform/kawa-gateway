package io.jonasg.kawa.virtualtopic.filter;

import org.apache.kafka.common.record.internal.Record;
import org.jspecify.annotations.Nullable;

/// The decoded value of one fetched record, decoded on the first [#get()] and remembered, so a
/// virtual topic's consume filter and its transformations decode each value at most once. A record
/// nobody asks the value of - one a header filter drops - is never decoded.
///
/// A value that does not decode is remembered as well: every [#get()] throws the same
/// [PayloadDecodeException]. A [SchemaRegistryUnavailableException] is not remembered: it is no
/// record error, so it propagates from the call that hit it.
///
/// The decoded tree is shared by everyone who reads it, so it must never be mutated: CEL only reads
/// it, and [io.jonasg.kawa.virtualtopic.mapping.ValueMapping]s return changed copies. Not thread-safe:
/// it lives for the evaluation of one record.
public final class DecodedValue {

    private final Record record;
    private final @Nullable PayloadDecoder decoder;
    private @Nullable Object value;
    private @Nullable PayloadDecodeException failure;
    private boolean decoded;

    /// @param decoder decodes the record value, `null` when the topic has no `valueFormat`: its
    ///                values are raw and never decoded
    public DecodedValue(Record record, @Nullable PayloadDecoder decoder) {
        this.record = record;
        this.decoder = decoder;
    }

    /// The decoded record value.
    ///
    /// @throws PayloadDecodeException             when the value does not decode, on this and every later call
    /// @throws SchemaRegistryUnavailableException when the schema registry is unavailable
    /// @throws IllegalStateException              when there is no decoder: raw values are never decoded
    public Object get() {
        if (!decoded) {
            if (decoder == null) {
                throw new IllegalStateException("The value of the record at offset " + record.offset() + " is raw, it has no format to decode it with");
            }
            try {
                value = decoder.decode(record.value());
            } catch (PayloadDecodeException e) {
                failure = e;
            }
            decoded = true;
        }
        if (failure != null) {
            throw failure;
        }
        return value;
    }
}

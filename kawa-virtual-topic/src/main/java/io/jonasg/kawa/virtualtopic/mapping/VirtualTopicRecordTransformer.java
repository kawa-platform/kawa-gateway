package io.jonasg.kawa.virtualtopic.mapping;

import io.jonasg.kawa.config.DecodeErrorPolicy;
import io.jonasg.kawa.config.PayloadFormatConfig;
import io.jonasg.kawa.virtualtopic.filter.AvroSchemaResolver;
import io.jonasg.kawa.virtualtopic.filter.AvroSubjectResolver;
import io.jonasg.kawa.virtualtopic.filter.DecodedValue;
import io.jonasg.kawa.virtualtopic.filter.FailedRecordException;
import io.jonasg.kawa.virtualtopic.filter.PayloadCodecs;
import io.jonasg.kawa.virtualtopic.filter.PayloadDecodeException;
import io.jonasg.kawa.virtualtopic.filter.PayloadDecoder;
import io.jonasg.kawa.virtualtopic.filter.PayloadEncodeException;
import io.jonasg.kawa.virtualtopic.filter.PayloadEncoder;
import io.jonasg.kawa.virtualtopic.filter.RecordBatches;
import io.jonasg.kawa.virtualtopic.filter.VirtualTopicRecordFilter;
import org.apache.kafka.common.record.internal.BaseRecords;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MutableRecordBatch;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.common.utils.CloseableIterator;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/// Applies a virtual topic's transformations to a fetched partition's records: each record value
/// is decoded with the topic's `valueFormat`, mapped, and encoded in its output format, and every
/// batch is rebuilt around the new values.
///
/// A topic that also has a consume filter is filtered in the same pass: each record is first asked
/// of the filter, and only the records it keeps are transformed. The filter and the
/// transformations decode with the same `valueFormat` and schemas, so they share one
/// [DecodedValue]: each value is decoded at most once, and a record a header filter drops is never
/// decoded. A value the filter could not decode but keeps (the `include` policy) is delivered as
/// stored, as any value that cannot be transformed under `include`.
///
/// Batches are rebuilt the way Kafka's own `MemoryRecords.filterTo` rebuilds the batches it
/// filters (log compaction): offsets, timestamps, keys and headers stay, as do the producer id,
/// epoch and base sequence, the transactional flag and the partition leader epoch; the batch keeps
/// its last offset, so a consumer moves past it even when its last records were dropped, and its
/// compression codec. Batches in the legacy formats (magic v0 and v1) come out as v2. So
/// `read_committed` consumers still drop aborted records (the aborted transactions of the
/// partition name producer ids and offsets, both preserved), and control batches - transaction
/// markers - are copied unchanged.
///
/// A trailing batch the broker cut off at the fetch size is not a complete batch, so it is left
/// out, as `filterTo` leaves it out; the consumer fetches it again from its position.
///
/// A tombstone (a record without a value) the filter keeps passes through unchanged. A record whose value cannot be
/// transformed - it does not decode, the mapping fails on it, or the result cannot be encoded - is
/// handled per the `onDecodeError` policy of the topic's `valueFormat`:
///
/// | Policy    | The consumer gets                                                                  |
/// |-----------|------------------------------------------------------------------------------------|
/// | `skip`    | nothing: the record is dropped, leaving an offset gap (the default)                |
/// | `include` | the stored value, untransformed - visibly not in the format it expects            |
/// | `fail`    | the records before it; the partition stops at it, as a `fail` filter does          |
///
/// A record the filter cannot decode is handled per the same policy; under `fail` the partition
/// stops at the first record, in offset order, that the filter or the transformations fail on.
///
/// A schema registry outage ([io.jonasg.kawa.virtualtopic.filter.SchemaRegistryUnavailableException]) is not a record error and
/// bypasses the policy: it is thrown, whatever the policy, so the partition fails instead of
/// records being dropped or delivered untransformed.
///
/// Every record of a transformed topic is decoded, mapped and re-encoded on every fetch: the cost
/// grows with the number and size of the records, and it is paid again by every consumer.
public final class VirtualTopicRecordTransformer {

    private static final Logger log = LoggerFactory.getLogger(VirtualTopicRecordTransformer.class);

    private final Transformation transformation;

    private VirtualTopicRecordTransformer(Transformation transformation) {
        this.transformation = transformation;
    }

    /// Prepares the transformations of one virtual topic - its value decoder, compiled mapping and
    /// encoder - once, so every fetch applies them without preparation work. Built for each virtual
    /// topic of a [io.jonasg.kawa.virtualtopic.VirtualTopicManager] snapshot, so it lives exactly as
    /// long as the configuration it was prepared for.
    ///
    /// @param valueFormat  how stored values are decoded; its `onDecodeError` policy handles a value
    ///                     that cannot be transformed
    /// @param mapping      the compiled transformations
    /// @param outputFormat the format transformed values are encoded in
    /// @param avroSchemas  the schemas of the physical topic's cluster, to decode Avro values with,
    ///                     `null` when that cluster has no schema registry
    /// @param avroSubjects the subjects of the physical topic's cluster, to encode Avro values with,
    ///                     `null` when that cluster has no schema registry
    public static VirtualTopicRecordTransformer of(
            PayloadFormatConfig valueFormat,
            ValueMapping mapping,
            PayloadFormatConfig outputFormat,
            @Nullable AvroSchemaResolver avroSchemas,
            @Nullable AvroSubjectResolver avroSubjects
    ) {
        var codecs = new Codecs(PayloadCodecs.decoder(valueFormat, avroSchemas), PayloadCodecs.encoder(outputFormat, avroSubjects));
        return new VirtualTopicRecordTransformer(new Transformation(codecs, mapping, valueFormat.onDecodeError()));
    }

    /// Returns the records of `records` that `filter` keeps, each value transformed, or `records`
    /// unchanged when there are no records (the fast path). Control batches are copied unchanged,
    /// never filtered or transformed.
    ///
    /// @param filter the topic's consume filter, `null` when it has none
    /// @throws FailedRecordException when a record cannot be filtered or transformed and the policy is
    ///                               `fail`: it carries the records before that record, filtered and transformed
    /// @throws io.jonasg.kawa.virtualtopic.filter.SchemaRegistryUnavailableException when the schema registry is unavailable, whatever the policy
    /// @throws IllegalStateException when the filter could not be prepared, or fails to evaluate
    public BaseRecords apply(@Nullable VirtualTopicRecordFilter filter, BaseRecords records) {
        if (!(records instanceof MemoryRecords memoryRecords) || memoryRecords.sizeInBytes() == 0) {
            return records;
        }
        var output = new ByteBufferOutputStream(memoryRecords.sizeInBytes());
        for (MutableRecordBatch batch : memoryRecords.batches()) {
            if (batch.isControlBatch()) {
                batch.writeTo(output);
                continue;
            }
            Stop stop = transformBatch(batch, filter, transformation, output);
            if (stop != null) {
                throw new FailedRecordException(stop.offset(), readable(output), stop.cause());
            }
        }
        return readable(output);
    }

    private static MemoryRecords readable(ByteBufferOutputStream output) {
        ByteBuffer buffer = output.buffer();
        buffer.flip();
        return MemoryRecords.readableRecords(buffer);
    }

    /// The offset of the record `batch` stopped at under the `fail` policy, or `null`. On a stop the
    /// records before it are written, ending right before it.
    private static @Nullable Stop transformBatch(
            MutableRecordBatch batch,
            @Nullable VirtualTopicRecordFilter filter,
            Transformation transformation,
            ByteBufferOutputStream output
    ) {
        List<RecordBatches.Entry> kept = new ArrayList<>();
        try (CloseableIterator<Record> records = batch.streamingIterator(BufferSupplier.NO_CACHING)) {
            while (records.hasNext()) {
                Record record = records.next();
                ByteBuffer value;
                try {
                    DecodedValue decoded = null;
                    if (filter != null) {
                        decoded = new DecodedValue(record, transformation.codecs().decoder());
                        if (!filter.matches(record, decoded)) {
                            continue;
                        }
                    }
                    if (!record.hasValue()) {
                        kept.add(new RecordBatches.Entry(record, null));
                        continue;
                    }
                    value = transformation.apply(record, decoded);
                } catch (PayloadDecodeException | PayloadMappingException | PayloadEncodeException e) {
                    // only escapes the filter or apply under the fail policy
                    if (!kept.isEmpty() || record.offset() > batch.baseOffset()) {
                        RecordBatches.write(batch, kept, record.offset() - 1, output);
                    }
                    return new Stop(record.offset(), e);
                }
                if (value != null) {
                    kept.add(new RecordBatches.Entry(record, value));
                }
            }
        }
        RecordBatches.write(batch, kept, batch.lastOffset(), output);
        return null;
    }

    private record Stop(long offset, RuntimeException cause) {
    }

    private record Transformation(Codecs codecs, ValueMapping mapping, DecodeErrorPolicy onError) {

        /// The transformed value of `record`; on failure its stored value or `null` (dropped) per
        /// `onError`, or the failure itself under `fail`. Any other exception is no record error and
        /// is not handled per `onError` either. A [io.jonasg.kawa.virtualtopic.filter.SchemaRegistryUnavailableException] is not caught: it is no record error.
        ///
        /// @param decoded the value of `record` the filter may already have decoded, `null` to decode it here
        @Nullable ByteBuffer apply(Record record, @Nullable DecodedValue decoded) {
            try {
                Object value = decoded == null ? codecs.decoder().decode(record.value()) : decoded.get();
                return ByteBuffer.wrap(codecs.encoder().encode(mapping.apply(value, record)));
            } catch (PayloadDecodeException | PayloadMappingException | PayloadEncodeException e) {
                return switch (onError) {
                    case SKIP -> {
                        log.debug("Dropping record at offset {}, its value cannot be transformed: {}", record.offset(), e.getMessage());
                        yield null;
                    }
                    case INCLUDE -> {
                        log.debug("Delivering record at offset {} untransformed: {}", record.offset(), e.getMessage());
                        yield record.value();
                    }
                    case FAIL -> throw e;
                };
            }
        }
    }

    private record Codecs(PayloadDecoder decoder, PayloadEncoder encoder) {
    }
}

package io.jonasg.kawa.virtualtopic.mapping;

import io.jonasg.kawa.config.ConvertTransformationConfig;
import io.jonasg.kawa.config.DecodeErrorPolicy;
import io.jonasg.kawa.config.DropHeadersTransformationConfig;
import io.jonasg.kawa.config.PayloadFormatConfig;
import io.jonasg.kawa.config.SetHeaderTransformationConfig;
import io.jonasg.kawa.config.TransformationConfig;
import io.jonasg.kawa.virtualtopic.filter.AvroSchemaResolver;
import io.jonasg.kawa.virtualtopic.filter.AvroSubjectResolver;
import io.jonasg.kawa.virtualtopic.filter.DecodedValue;
import io.jonasg.kawa.virtualtopic.filter.ErrorHeaders;
import io.jonasg.kawa.virtualtopic.filter.FailedRecordException;
import io.jonasg.kawa.virtualtopic.filter.PayloadCodecs;
import io.jonasg.kawa.virtualtopic.filter.PayloadDecodeException;
import io.jonasg.kawa.virtualtopic.filter.PayloadDecoder;
import io.jonasg.kawa.virtualtopic.filter.PayloadEncodeException;
import io.jonasg.kawa.virtualtopic.filter.PayloadEncoder;
import io.jonasg.kawa.virtualtopic.filter.RecordBatches;
import io.jonasg.kawa.virtualtopic.filter.Verdict;
import io.jonasg.kawa.virtualtopic.filter.VirtualTopicRecordFilter;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.internal.BaseRecords;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MutableRecordBatch;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.utils.internals.BufferSupplier;
import org.apache.kafka.common.utils.internals.ByteBufferOutputStream;
import org.apache.kafka.common.utils.internals.CloseableIterator;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/// Applies a virtual topic's transformations to a fetched partition's records: each record value
/// is decoded with the topic's `valueFormat`, mapped, and encoded in its output format, its
/// headers are mapped by the header steps, and every batch is rebuilt around the new records.
///
/// Headers describe the delivered bytes. The header steps run on every record with a value, except
/// a value delivered as stored after a failed transformation when the topic has a `convert`: that
/// value is still in the stored format, so its headers stay. A tombstone keeps its headers.
///
/// A topic whose steps are header steps only (no value step, no `convert`) decodes and re-encodes
/// nothing: its values pass through as stored, and it needs no `valueFormat` unless it filters.
///
/// A topic that also has a consume filter is filtered in the same pass: each record is first asked
/// of the filter, and only the records it keeps are transformed. The filter and the
/// transformations decode with the same `valueFormat` and schemas, so they share one
/// [DecodedValue]: each value is decoded at most once, and a record a header filter drops is never
/// decoded. A value the filter could not decode but keeps (the `include` policy) is delivered as
/// stored, as any value that cannot be transformed under `include`; it carries the filter's
/// [ErrorHeaders], and the value steps are not applied to it.
///
/// Batches are rebuilt the way Kafka's own `MemoryRecords.filterTo` rebuilds the batches it
/// filters (log compaction): offsets, timestamps and keys stay, as do the producer id,
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
/// | `include` | the stored value, untransformed, with the [ErrorHeaders] that say why              |
/// | `fail`    | the records before it; the partition stops at it, as a `fail` filter does          |
///
/// The error headers name the failing step: a value that does not decode fails the first value
/// step, one whose result does not encode the `convert` step (the last value step without one).
/// Header steps never fail. A step with `errorHeader: false` delivers the records it fails on
/// without them. They are added after the header steps ran, so no header step removes or forges them.
///
/// A record the filter cannot decode is handled per the same policy; under `fail` the partition
/// stops at the first record, in offset order, that the filter or the transformations fail on.
///
/// A schema registry outage ([io.jonasg.kawa.virtualtopic.filter.SchemaRegistryUnavailableException]) is not a record error and
/// bypasses the policy: it is thrown, whatever the policy, so the partition fails instead of
/// records being dropped or delivered untransformed.
///
/// Every record of a topic with value steps or a `convert` is decoded, mapped and re-encoded on every
/// fetch: the cost grows with the number and size of the records, and it is paid again by every
/// consumer. A topic with only header steps pays none of it.
public final class VirtualTopicRecordTransformer {

    private static final Logger log = LoggerFactory.getLogger(VirtualTopicRecordTransformer.class);

    private final Transformation transformation;

    private VirtualTopicRecordTransformer(Transformation transformation) {
        this.transformation = transformation;
    }

    /// Prepares the transformations of one virtual topic - its value decoder, compiled mappings and
    /// encoder - once, so every fetch applies them without preparation work. Built for each virtual
    /// topic of a [io.jonasg.kawa.virtualtopic.VirtualTopicManager] snapshot, so it lives exactly as
    /// long as the configuration it was prepared for.
    ///
    /// @param valueFormat  how stored values are decoded, `null` when the topic has none: then it has
    ///                     only header steps, and no value is decoded
    /// @param mapping      the compiled value steps
    /// @param headers      the compiled header steps
    /// @param outputFormat the format delivered values are in, `null` only without a `valueFormat`
    /// @param avroSchemas  the schemas of the physical topic's cluster, to decode Avro values with,
    ///                     `null` when that cluster has no schema registry
    /// @param avroSubjects the subjects of the physical topic's cluster, to encode Avro values with,
    ///                     `null` when that cluster has no schema registry
    /// @param steps        the transformations `mapping` and `headers` were compiled from, for the
    ///                     `errorHeader` of the step a failure is blamed on
    public static VirtualTopicRecordTransformer of(
            @Nullable PayloadFormatConfig valueFormat,
            ValueMapping mapping,
            HeaderMapping headers,
            @Nullable PayloadFormatConfig outputFormat,
            @Nullable AvroSchemaResolver avroSchemas,
            @Nullable AvroSubjectResolver avroSubjects,
            List<TransformationConfig> steps
    ) {
        boolean sameFormat = Objects.equals(outputFormat, valueFormat);
        boolean headersOnly = mapping == ValueMapping.IDENTITY && sameFormat;
        PayloadDecoder decoder = valueFormat == null ? null : PayloadCodecs.decoder(valueFormat, avroSchemas);
        PayloadEncoder encoder = headersOnly || outputFormat == null
                ? null
                : PayloadCodecs.encoder(outputFormat, avroSubjects);
        var codecs = new Codecs(decoder, encoder);
        DecodeErrorPolicy onError = valueFormat == null ? DecodeErrorPolicy.FAIL : valueFormat.onDecodeError();
        var blame = new Blame(steps.stream().map(TransformationConfig::errorHeader).toList(), decodeStep(steps), encodeStep(steps));
        return new VirtualTopicRecordTransformer(new Transformation(codecs, mapping, headers, onError, sameFormat, headersOnly, blame));
    }

    /// The step a value that does not decode fails: the first that reads the value.
    private static int decodeStep(List<TransformationConfig> steps) {
        for (int i = 0; i < steps.size(); i++) {
            if (!isHeaderStep(steps.get(i))) {
                return i;
            }
        }
        return 0;
    }

    /// The step a result that does not encode fails: the `convert` step, else the last value step.
    private static int encodeStep(List<TransformationConfig> steps) {
        for (int i = 0; i < steps.size(); i++) {
            if (steps.get(i) instanceof ConvertTransformationConfig) {
                return i;
            }
        }
        for (int i = steps.size() - 1; i >= 0; i--) {
            if (!isHeaderStep(steps.get(i))) {
                return i;
            }
        }
        return steps.size() - 1;
    }

    private static boolean isHeaderStep(TransformationConfig step) {
        return step instanceof DropHeadersTransformationConfig || step instanceof SetHeaderTransformationConfig;
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
                RecordBatches.Entry transformed;
                try {
                    DecodedValue decoded = null;
                    if (filter != null) {
                        decoded = new DecodedValue(record, transformation.codecs().decoder());
                        Verdict verdict = filter.verdict(record, decoded);
                        if (!verdict.keep()) {
                            continue;
                        }
                        if (verdict.failure() != null && record.hasValue()) {
                            // the value steps would fail on the same value: deliver it as stored
                            kept.add(transformation.deliveredAsStored(record, verdict));
                            continue;
                        }
                    }
                    if (!record.hasValue()) {
                        kept.add(new RecordBatches.Entry(record, null));
                        continue;
                    }
                    transformed = transformation.apply(record, decoded);
                } catch (PayloadDecodeException | PayloadMappingException | PayloadEncodeException e) {
                    // only escapes the filter or apply under the fail policy
                    if (!kept.isEmpty() || record.offset() > batch.baseOffset()) {
                        RecordBatches.write(batch, kept, record.offset() - 1, output);
                    }
                    return new Stop(record.offset(), e);
                }
                if (transformed != null) {
                    kept.add(transformed);
                }
            }
        }
        RecordBatches.write(batch, kept, batch.lastOffset(), output);
        return null;
    }

    private record Stop(long offset, RuntimeException cause) {
    }

    /// Which step a failure is blamed on, and whether it writes the error headers.
    ///
    /// @param errorHeaders the `errorHeader` of each step, by index
    /// @param decodeStep   the step a value that does not decode fails
    /// @param encodeStep   the step a result that does not encode fails
    private record Blame(List<Boolean> errorHeaders, int decodeStep, int encodeStep) {

        /// `headers` with the [ErrorHeaders] of `failure` at `step`, unless that step turns them off.
        Header[] headers(Header[] headers, RuntimeException failure, int step) {
            return errorHeaders.get(step) ? ErrorHeaders.with(headers, failure, ErrorHeaders.TRANSFORMATION_STAGE, step) : headers;
        }
    }

    private record Transformation(
            Codecs codecs,
            ValueMapping mapping,
            HeaderMapping headers,
            DecodeErrorPolicy onError,
            boolean sameFormat,
            boolean headersOnly,
            Blame blame
    ) {

        /// `record` as stored, after the filter failed on it under `include`: the header steps
        /// run when the stored value is in the delivered format, then the filter's [ErrorHeaders]
        /// are added.
        RecordBatches.Entry deliveredAsStored(Record record, Verdict verdict) {
            Header[] mapped = sameFormat ? headers.apply(record.headers()) : record.headers();
            return new RecordBatches.Entry(record, record.value(), verdict.headers(mapped));
        }

        /// `record` with its transformed value and mapped headers; on failure `record` as stored or
        /// `null` (dropped) per `onError`, or the failure itself under `fail`. Any other exception is no record error and
        /// is not handled per `onError` either. A [io.jonasg.kawa.virtualtopic.filter.SchemaRegistryUnavailableException] is not caught: it is no record error.
        ///
        /// A header-only topic maps the headers of `record` and keeps its value bytes; nothing is decoded.
        ///
        /// @param decoded the value of `record` the filter may already have decoded, `null` to decode it here
        RecordBatches.@Nullable Entry apply(Record record, @Nullable DecodedValue decoded) {
            if (headersOnly) {
                return new RecordBatches.Entry(record, record.value(), headers.apply(record.headers()));
            }
            try {
                Object value = decoded == null ? codecs.decoder().decode(record.value()) : decoded.get();
                ByteBuffer encoded = ByteBuffer.wrap(codecs.encoder().encode(mapping.apply(value, record)));
                return new RecordBatches.Entry(record, encoded, headers.apply(record.headers()));
            } catch (PayloadDecodeException e) {
                return failed(record, e, blame.decodeStep());
            } catch (PayloadMappingException e) {
                return failed(record, e, e.step() == PayloadMappingException.UNKNOWN_STEP ? blame.decodeStep() : e.step());
            } catch (PayloadEncodeException e) {
                return failed(record, e, blame.encodeStep());
            }
        }

        /// @param step the index of the step the failure is blamed on
        private RecordBatches.@Nullable Entry failed(Record record, RuntimeException e, int step) {
            return switch (onError) {
                case SKIP -> {
                    log.debug("Dropping record at offset {}, its value cannot be transformed: {}", record.offset(), e.getMessage());
                    yield null;
                }
                case INCLUDE -> {
                    log.debug("Delivering record at offset {} untransformed: {}", record.offset(), e.getMessage());
                    // the stored value is in the delivered format only when no convert changed it
                    Header[] mapped = sameFormat ? headers.apply(record.headers()) : record.headers();
                    yield new RecordBatches.Entry(record, record.value(), blame.headers(mapped, e, step));
                }
                case FAIL -> throw e;
            };
        }
    }

    /// The codecs of a topic. The decoder is `null` without a `valueFormat`, the encoder when the
    /// topic only maps headers.
    private record Codecs(@Nullable PayloadDecoder decoder, @Nullable PayloadEncoder encoder) {
    }

}

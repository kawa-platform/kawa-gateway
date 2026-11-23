package io.jonasg.kawa.virtualtopic.filter;

import io.jonasg.kawa.config.CelFilterConfig;
import io.jonasg.kawa.config.DecodeErrorPolicy;
import io.jonasg.kawa.config.HeaderContainsFilterConfig;
import io.jonasg.kawa.config.HeaderEqualsFilterConfig;
import io.jonasg.kawa.config.HeaderMatchesFilterConfig;
import io.jonasg.kawa.config.HeaderStartsWithFilterConfig;
import io.jonasg.kawa.config.PayloadFormatConfig;
import io.jonasg.kawa.config.VirtualTopicFilterConfig;
import org.apache.kafka.common.record.internal.BaseRecords;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.record.internal.Records;
import org.apache.kafka.common.utils.internals.BufferSupplier;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Objects;

/// Applies a virtual topic's configured consume filter to a fetched partition's records:
/// decodes the batch(es), drops records the filter rejects, and re-encodes a valid batch.
///
/// Built on Kafka's own [MemoryRecords#filterTo(MemoryRecords.RecordFilter, ByteBuffer, BufferSupplier, int)],
/// the same decode-filter-reencode
/// mechanism the broker's log cleaner (compaction) and client-side down-conversion use to drop
/// records from a batch and re-emit a valid one - rather than hand-rolling batch encoding. A batch
/// whose records are all kept is copied as it is.
///
/// Control batches - transaction markers - are delivered unchanged: the filter never evaluates
/// them. A batch the filter drops every record of stays as an empty batch, keeping its offset range
/// and producer state; one in the legacy formats (magic v0 and v1) cannot be empty, so it is left
/// out. A legacy batch the filter keeps records of comes out as v2.
///
/// A topic that also has transformations is not filtered here: the
/// [io.jonasg.kawa.virtualtopic.mapping.VirtualTopicRecordTransformer] filters and transforms its
/// records in one pass, asking [#matches(Record, DecodedValue)] of each record.
///
/// A record whose value does not decode is handled per the format's [DecodeErrorPolicy]; under
/// `fail` the partition stops at it, see [FailedRecordException]. A schema
/// registry outage ([SchemaRegistryUnavailableException]) is not a record error and bypasses the
/// policy: it is thrown whatever the policy, so the partition fails instead of records being
/// dropped or included.
/// Surviving records keep their original offsets (this is what makes offset gaps in a fetch
/// response safe for consumers, the same property compacted topics already rely on).
public final class VirtualTopicRecordFilter {

    private static final Logger log = LoggerFactory.getLogger(VirtualTopicRecordFilter.class);

    private final @Nullable EvaluatingRecordFilter recordFilter;
    private final @Nullable IllegalStateException invalid;

    private VirtualTopicRecordFilter(@Nullable EvaluatingRecordFilter recordFilter, @Nullable IllegalStateException invalid) {
        this.recordFilter = recordFilter;
        this.invalid = invalid;
    }

    /// Prepares the consume filter of one virtual topic - e.g. compiles its CEL expression - once,
    /// so every fetch evaluates it without preparation work. Built for each virtual topic of a
    /// [io.jonasg.kawa.virtualtopic.VirtualTopicManager] snapshot, so it lives exactly as long as
    /// the configuration it was prepared for.
    ///
    /// A filter that cannot be prepared does not fail here, so it never refuses the rest of the
    /// configuration: every fetch of that topic's records fails with it instead.
    ///
    /// @param virtualTopic the virtual topic the filter belongs to, to name in failures
    /// @param valueFormat  how record values are decoded, `null` for raw values
    /// @param avroSchemas  the schemas of the physical topic's cluster, to decode Avro values with;
    ///                     `null` when that cluster has none, in which case Avro values fail to decode
    ///                     and are handled per the format's [DecodeErrorPolicy]
    public static VirtualTopicRecordFilter of(
            String virtualTopic,
            VirtualTopicFilterConfig filter,
            @Nullable PayloadFormatConfig valueFormat,
            @Nullable AvroSchemaResolver avroSchemas
    ) {
        try {
            return new VirtualTopicRecordFilter(new EvaluatingRecordFilter(filter, valueFormat, avroSchemas), null);
        } catch (RuntimeException e) {
            log.warn("The filter of virtual topic '{}' is invalid, every fetch of its records fails: {}", virtualTopic, e.getMessage());
            return new VirtualTopicRecordFilter(null,
                    new IllegalStateException("The filter of virtual topic '" + virtualTopic + "' is invalid: " + e.getMessage(), e));
        }
    }

    /// Returns `records` filtered, or `records` unchanged when there is nothing to filter (null or
    /// empty - the fast path most partitions take).
    ///
    /// @throws FailedRecordException when a record value does not decode under the `fail` policy: it
    ///                               carries the filtered records before that record
    /// @throws IllegalStateException when the filter could not be prepared
    public BaseRecords apply(BaseRecords records) {
        if (!(records instanceof MemoryRecords memoryRecords) || memoryRecords.sizeInBytes() == 0) {
            return records;
        }
        EvaluatingRecordFilter prepared = prepared();
        try {
            return filter(memoryRecords, prepared);
        } catch (Stop stop) {
            // The records before the bad one passed, so filtering them again cannot stop.
            MemoryRecords before = RecordBatches.before(memoryRecords, stop.offset);
            MemoryRecords filtered = before.sizeInBytes() == 0 ? before : filter(before, prepared);
            throw new FailedRecordException(stop.offset, filtered, stop.failure);
        }
    }

    /// Whether the filter keeps `record`, a record of a data batch, reading its value from `value`:
    /// a value that does not decode is handled per the format's [DecodeErrorPolicy] - dropped
    /// (`skip`), kept (`include`) or thrown (`fail`). The value is only decoded when the filter reads
    /// it, so a header filter never decodes it.
    ///
    /// @param value the record's value, decoded with the topic's `valueFormat` and the physical
    ///              topic's schemas, as this filter decodes it
    /// @throws PayloadDecodeException             when the value does not decode under the `fail` policy
    /// @throws SchemaRegistryUnavailableException when the schema registry is unavailable, whatever the policy
    /// @throws IllegalStateException              when the filter could not be prepared, or fails to evaluate
    public boolean matches(Record record, DecodedValue value) {
        return prepared().matches(record, value);
    }

    private EvaluatingRecordFilter prepared() {
        if (recordFilter == null) {
            throw Objects.requireNonNull(invalid);
        }
        return recordFilter;
    }

    private static MemoryRecords filter(MemoryRecords records, EvaluatingRecordFilter recordFilter) {
        ByteBuffer output = ByteBuffer.allocate(records.sizeInBytes());
        records.filterTo(recordFilter, output, BufferSupplier.NO_CACHING, Records.SOFT_MAX_ARRAY_LENGTH);
        output.flip();
        return MemoryRecords.readableRecords(output);
    }

    /// A record the `fail` policy stops the partition at, thrown out of `filterTo`.
    private static final class Stop extends RuntimeException {

        private final long offset;
        private final PayloadDecodeException failure;

        private Stop(long offset, PayloadDecodeException failure) {
            super(failure.getMessage(), failure, false, false);
            this.offset = offset;
            this.failure = failure;
        }
    }

    /// Retains a record if it matches the configured filter. The [RecordPredicate] is resolved
    /// once at construction from the sealed [VirtualTopicFilterConfig], so per-record evaluation
    /// does no preparation work and has no wire-encoding concerns. The switch below is
    /// exhaustive over the sealed interface's permitted subtypes: adding a new filter kind is a
    /// compile error here until a case is added.
    static final class EvaluatingRecordFilter extends MemoryRecords.RecordFilter {

        private static final BatchRetentionResult RETAIN_EMPTY = new BatchRetentionResult(BatchRetention.RETAIN_EMPTY, false);
        private static final BatchRetentionResult DELETE_EMPTY = new BatchRetentionResult(BatchRetention.DELETE_EMPTY, false);

        private final RecordPredicate predicate;
        private final @Nullable PayloadDecoder valueDecoder;
        private final DecodeErrorPolicy onDecodeError;

        EvaluatingRecordFilter(VirtualTopicFilterConfig filterCfg) {
            this(filterCfg, null);
        }

        EvaluatingRecordFilter(VirtualTopicFilterConfig filterCfg, @Nullable PayloadFormatConfig valueFormat) {
            this(filterCfg, valueFormat, null);
        }

        /// @param avroSchemas the schemas of the physical topic's cluster, for the [io.jonasg.kawa.config.AvroFormatConfig]
        ///                    format; `null` when that cluster has no schema registry
        EvaluatingRecordFilter(VirtualTopicFilterConfig filterCfg, @Nullable PayloadFormatConfig valueFormat, @Nullable AvroSchemaResolver avroSchemas) {
            super(RecordBatch.NO_TIMESTAMP, -1L);
            this.valueDecoder = valueFormat == null ? null : PayloadCodecs.decoder(valueFormat, avroSchemas);
            this.predicate = predicateFor(filterCfg, valueDecoder != null);
            this.onDecodeError = valueFormat == null ? DecodeErrorPolicy.FAIL : valueFormat.onDecodeError();
        }

        @Override
        protected BatchRetentionResult checkBatchRetention(RecordBatch batch) {
            // Retain the batch even if every record in it is filtered out: an empty batch is still
            // a valid batch, and retaining it preserves producer id/epoch and sequence continuity
            // for idempotent/transactional producers. Never DELETE here - that would drop batch
            // metadata a consumer may depend on. A legacy batch cannot be empty, so filterTo leaves
            // it out once every record is filtered out.
            return batch.magic() < RecordBatch.MAGIC_VALUE_V2 ? DELETE_EMPTY : RETAIN_EMPTY;
        }

        @Override
        protected boolean shouldRetainRecord(
                RecordBatch batch,
                Record record
        ) {
            if (batch.isControlBatch()) {
                // a transaction marker is no record of the topic: it passes, and its batch is copied
                return true;
            }
            try {
                return matches(record, new DecodedValue(record, valueDecoder));
            } catch (PayloadDecodeException e) {
                // only escapes matches under the fail policy
                throw new Stop(record.offset(), e);
            }
        }

        /// A [SchemaRegistryUnavailableException] is not caught: it is no record error, and neither is
        /// any other exception, such as a filter that fails to evaluate.
        boolean matches(Record record, DecodedValue value) {
            try {
                return predicate.test(record, value);
            } catch (PayloadDecodeException e) {
                return switch (onDecodeError) {
                    case SKIP -> {
                        log.debug("Dropping record at offset {}: {}", record.offset(), e.getMessage());
                        yield false;
                    }
                    case INCLUDE -> true;
                    case FAIL -> throw e;
                };
            }
        }

        private static RecordPredicate predicateFor(VirtualTopicFilterConfig filterCfg, boolean decoded) {
            return switch (filterCfg) {
                case HeaderEqualsFilterConfig cfg -> new HeaderEqualsRecordPredicate(cfg);
                case HeaderContainsFilterConfig cfg -> new HeaderContainsRecordPredicate(cfg);
                case HeaderStartsWithFilterConfig cfg -> new HeaderStartsWithRecordPredicate(cfg);
                case HeaderMatchesFilterConfig cfg -> new HeaderMatchesRecordPredicate(cfg);
                case CelFilterConfig cfg -> new CelRecordPredicate(cfg, decoded);
            };
        }

    }
}

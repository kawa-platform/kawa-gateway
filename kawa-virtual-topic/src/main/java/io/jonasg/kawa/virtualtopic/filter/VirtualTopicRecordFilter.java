package io.jonasg.kawa.virtualtopic.filter;

import io.jonasg.kawa.config.CelFilterConfig;
import io.jonasg.kawa.config.DecodeErrorPolicy;
import io.jonasg.kawa.config.HeaderContainsFilterConfig;
import io.jonasg.kawa.config.HeaderEqualsFilterConfig;
import io.jonasg.kawa.config.HeaderMatchesFilterConfig;
import io.jonasg.kawa.config.HeaderStartsWithFilterConfig;
import io.jonasg.kawa.config.JsonFormatConfig;
import io.jonasg.kawa.config.PayloadFormatConfig;
import io.jonasg.kawa.config.VirtualTopicFilterConfig;
import org.apache.kafka.common.record.internal.BaseRecords;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.utils.BufferSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/// Applies a virtual topic's configured consume filter to a fetched partition's records:
/// decodes the batch(es), drops records the filter rejects, and re-encodes a valid batch.
///
/// Built on Kafka's own [MemoryRecords#filterTo(TopicPartition, MemoryRecords.RecordFilter,
/// ByteBuffer, int, BufferSupplier)], the same decode-filter-reencode
/// mechanism the broker's log cleaner (compaction) and client-side down-conversion use to drop
/// records from a batch and re-emit a valid one - rather than hand-rolling batch encoding.
/// Surviving records keep their original offsets (this is what makes offset gaps in a fetch
/// response safe for consumers, the same property compacted topics already rely on).
public final class VirtualTopicRecordFilter {

    private static final Logger LOG = LoggerFactory.getLogger(VirtualTopicRecordFilter.class);

    /// Evaluating filters keyed by config. Filter configs are records (value equality), so each
    /// distinct config is prepared - e.g. its CEL expression compiled - once and then reused across
    /// fetches, instead of being rebuilt for every fetched partition.
    private final Map<FilterKey, EvaluatingRecordFilter> filters = new ConcurrentHashMap<>();

    private record FilterKey(VirtualTopicFilterConfig filter, PayloadFormatConfig valueFormat) {
    }

    /// Returns `records` filtered per `filter` - with record values decoded per `valueFormat`
    /// when one is configured (`null` for raw values) - or `records` unchanged when
    /// there is nothing to decode (null or empty - the fast path most partitions take).
    public BaseRecords apply(
            VirtualTopicFilterConfig filter,
            PayloadFormatConfig valueFormat,
            BaseRecords records
    ) {
        if (!(records instanceof MemoryRecords memoryRecords) || memoryRecords.sizeInBytes() == 0) {
            return records;
        }

        ByteBuffer output = ByteBuffer.allocate(memoryRecords.sizeInBytes());
        memoryRecords.filterTo(
                filters.computeIfAbsent(
                        new FilterKey(filter, valueFormat),
                        key -> new EvaluatingRecordFilter(key.filter(), key.valueFormat())),
                output,
                BufferSupplier.NO_CACHING);
        output.flip();
        return MemoryRecords.readableRecords(output);
    }

    /// Retains a record if it matches the configured filter. The [RecordPredicate] is resolved
    /// once at construction from the sealed [VirtualTopicFilterConfig], so per-record evaluation
    /// does no preparation work and has no wire-encoding concerns. The switch below is
    /// exhaustive over the sealed interface's permitted subtypes: adding a new filter kind is a
    /// compile error here until a case is added.
    static final class EvaluatingRecordFilter extends MemoryRecords.RecordFilter {

        private final RecordPredicate predicate;
        private final DecodeErrorPolicy onDecodeError;

        EvaluatingRecordFilter(VirtualTopicFilterConfig filterCfg) {
            this(filterCfg, null);
        }

        EvaluatingRecordFilter(VirtualTopicFilterConfig filterCfg, PayloadFormatConfig valueFormat) {
            super(RecordBatch.NO_TIMESTAMP, -1L);
            this.predicate = predicateFor(filterCfg, decoderFor(valueFormat));
            this.onDecodeError = valueFormat == null ? DecodeErrorPolicy.FAIL : valueFormat.onDecodeError();
        }

        @Override
        protected BatchRetentionResult checkBatchRetention(RecordBatch batch) {
            // Always retain the batch, even if every record in it is filtered out: an empty
            // batch is still a valid batch, and retaining it preserves producer id/epoch and
            // sequence continuity for idempotent/transactional producers. Never DELETE here -
            // that would drop batch metadata a consumer may depend on.
            return new BatchRetentionResult(BatchRetention.RETAIN_EMPTY, false);
        }

        @Override
        protected boolean shouldRetainRecord(
                RecordBatch batch,
                Record record
        ) {
            return matches(record);
        }

        boolean matches(Record record) {
            try {
                return predicate.test(record);
            } catch (PayloadDecodeException e) {
                return switch (onDecodeError) {
                    case SKIP -> {
                        LOG.debug("Dropping record at offset {}: {}", record.offset(), e.getMessage());
                        yield false;
                    }
                    case INCLUDE -> true;
                    case FAIL -> throw e;
                };
            }
        }

        private static PayloadDecoder decoderFor(PayloadFormatConfig valueFormat) {
            return switch (valueFormat) {
                case null -> null;
                case JsonFormatConfig _ -> new JsonPayloadDecoder();
            };
        }

        private static RecordPredicate predicateFor(VirtualTopicFilterConfig filterCfg, PayloadDecoder valueDecoder) {
            return switch (filterCfg) {
                case HeaderEqualsFilterConfig cfg -> new HeaderEqualsRecordPredicate(cfg);
                case HeaderContainsFilterConfig cfg -> new HeaderContainsRecordPredicate(cfg);
                case HeaderStartsWithFilterConfig cfg -> new HeaderStartsWithRecordPredicate(cfg);
                case HeaderMatchesFilterConfig cfg -> new HeaderMatchesRecordPredicate(cfg);
                case CelFilterConfig cfg -> new CelRecordPredicate(cfg, valueDecoder);
            };
        }

    }
}

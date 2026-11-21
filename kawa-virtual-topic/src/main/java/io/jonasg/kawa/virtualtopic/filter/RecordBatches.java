package io.jonasg.kawa.virtualtopic.filter;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.internal.DefaultRecordBatch;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MemoryRecordsBuilder;
import org.apache.kafka.common.record.internal.MutableRecordBatch;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.common.utils.CloseableIterator;
import org.jspecify.annotations.Nullable;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/// Rebuilds record batches the way Kafka's own `MemoryRecords.filterTo` rebuilds the batches it
/// filters (log compaction): offsets, timestamps, keys and headers stay, as do the producer id,
/// epoch and base sequence, the transactional flag, the partition leader epoch and the compression
/// codec. Batches in the legacy formats (magic v0 and v1) come out as v2.
public final class RecordBatches {

    private RecordBatches() {
    }

    /// A record to write, with the value to write it with (`null` for a tombstone).
    public record Entry(Record record, @Nullable ByteBuffer value) {
    }

    /// The records of `records` before `offset`: the batches that end before it, unchanged, and the
    /// records of the batch holding it that come before it, as a batch that ends right before
    /// `offset` so a consumer's next fetch starts there. Empty when `offset` is the first record.
    public static MemoryRecords before(MemoryRecords records, long offset) {
        var output = new ByteBufferOutputStream(records.sizeInBytes());
        for (MutableRecordBatch batch : records.batches()) {
            if (batch.lastOffset() < offset) {
                batch.writeTo(output);
                continue;
            }
            if (batch.baseOffset() < offset) {
                List<Entry> kept = new ArrayList<>();
                try (CloseableIterator<Record> iterator = batch.streamingIterator(BufferSupplier.NO_CACHING)) {
                    while (iterator.hasNext()) {
                        Record record = iterator.next();
                        if (record.offset() >= offset) {
                            break;
                        }
                        kept.add(new Entry(record, record.value()));
                    }
                }
                write(batch, kept, offset - 1, output);
            }
            break;
        }
        ByteBuffer buffer = output.buffer();
        buffer.flip();
        return MemoryRecords.readableRecords(buffer);
    }

    /// Writes `records` as a batch with the metadata of `original`, ending at `lastOffset`, mirroring
    /// `filterTo`'s `buildRetainedRecordsInto`. Without records it writes an empty batch, as
    /// `filterTo` keeps one, so the consumer still moves past its offsets and producer state is kept;
    /// a legacy batch cannot be empty, so it is left out. A legacy batch ends at its last record.
    public static void write(RecordBatch original, List<Entry> records, long lastOffset, ByteBufferOutputStream output) {
        boolean legacy = original.magic() < RecordBatch.MAGIC_VALUE_V2;
        if (records.isEmpty()) {
            if (!legacy) {
                output.ensureRemaining(DefaultRecordBatch.RECORD_BATCH_OVERHEAD);
                DefaultRecordBatch.writeEmptyHeader(output.buffer(), RecordBatch.CURRENT_MAGIC_VALUE, original.producerId(),
                        original.producerEpoch(), original.baseSequence(), original.baseOffset(), lastOffset,
                        original.partitionLeaderEpoch(), original.timestampType(), original.maxTimestamp(),
                        original.isTransactional(), original.isControlBatch());
            }
            return;
        }
        // v0 has no timestamp type: CREATE_TIME without timestamps, as filterTo up-converts it
        TimestampType timestampType = original.timestampType() == TimestampType.NO_TIMESTAMP_TYPE
                ? TimestampType.CREATE_TIME
                : original.timestampType();
        long logAppendTime = timestampType == TimestampType.LOG_APPEND_TIME ? original.maxTimestamp() : RecordBatch.NO_TIMESTAMP;
        long baseOffset = legacy ? records.getFirst().record().offset() : original.baseOffset();
        try (var builder = new MemoryRecordsBuilder(output, RecordBatch.CURRENT_MAGIC_VALUE,
                Compression.of(original.compressionType()).build(), timestampType, baseOffset, logAppendTime,
                original.producerId(), original.producerEpoch(), original.baseSequence(), original.isTransactional(),
                false, original.partitionLeaderEpoch(), output.limit(),
                original.deleteHorizonMs().orElse(RecordBatch.NO_TIMESTAMP))) {
            for (Entry entry : records) {
                Record record = entry.record();
                builder.appendWithOffset(record.offset(), record.timestamp(), record.key(), entry.value(), record.headers());
            }
            if (!legacy) {
                builder.overrideLastOffset(lastOffset);
            }
            builder.build();
        }
    }
}

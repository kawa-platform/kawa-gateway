package io.jonasg.kawa.virtualtopic.filter;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.internal.DefaultRecordBatch;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MemoryRecordsBuilder;
import org.apache.kafka.common.record.internal.MutableRecordBatch;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.utils.internals.BufferSupplier;
import org.apache.kafka.common.utils.internals.ByteBufferOutputStream;
import org.apache.kafka.common.utils.internals.CloseableIterator;
import org.jspecify.annotations.Nullable;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/// Rebuilds record batches the way Kafka's own `MemoryRecords.filterTo` rebuilds the batches it
/// filters (log compaction): offsets, timestamps, keys and headers (unless an [Entry] replaces
/// them) stay, as do the producer id, epoch and base sequence, the transactional flag, the
/// partition leader epoch and the compression codec. Batches in the legacy formats (magic v0 and v1) come out as v2.
public final class RecordBatches {

    private RecordBatches() {
    }

    /// A record to write, with the value (`null` for a tombstone) and headers to write it with.
    public record Entry(Record record, @Nullable ByteBuffer value, Header[] headers) {

        /// The record with `value` and its own headers.
        public Entry(Record record, @Nullable ByteBuffer value) {
            this(record, value, record.headers());
        }
    }

    /// The records of `records` a consumer fetching at `fetchOffset` reads: the broker returns whole
    /// batches, starting at the one holding `fetchOffset`, and the consumer discards the records below
    /// it. Those are left out, so they are never filtered or transformed - a bad record the consumer
    /// already moved past must not stop it again: batches that end below `fetchOffset` are dropped and
    /// the batch holding it is rebuilt to start at its first record from `fetchOffset` on, keeping its
    /// last offset. Control batches pass through unchanged. `records` itself is returned when it
    /// already starts at or after `fetchOffset` (the common case).
    public static MemoryRecords from(MemoryRecords records, long fetchOffset) {
        if (startsFrom(records, fetchOffset)) {
            return records;
        }
        var output = new ByteBufferOutputStream(records.sizeInBytes());
        for (MutableRecordBatch batch : records.batches()) {
            if (batch.isControlBatch() || batch.baseOffset() >= fetchOffset) {
                batch.writeTo(output);
            } else if (batch.lastOffset() >= fetchOffset) {
                List<Entry> kept = new ArrayList<>();
                try (CloseableIterator<Record> iterator = batch.streamingIterator(BufferSupplier.NO_CACHING)) {
                    while (iterator.hasNext()) {
                        Record record = iterator.next();
                        if (record.offset() >= fetchOffset) {
                            kept.add(new Entry(record, record.value()));
                        }
                    }
                }
                long baseOffset = kept.isEmpty() ? fetchOffset : kept.getFirst().record().offset();
                write(batch, kept, baseOffset, batch.lastOffset(), output);
            }
        }
        ByteBuffer buffer = output.buffer();
        buffer.flip();
        return MemoryRecords.readableRecords(buffer);
    }

    private static boolean startsFrom(MemoryRecords records, long fetchOffset) {
        for (MutableRecordBatch batch : records.batches()) {
            if (!batch.isControlBatch()) {
                return batch.baseOffset() >= fetchOffset;
            }
            if (batch.lastOffset() >= fetchOffset) {
                return true;
            }
        }
        return true;
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
        long baseOffset = original.magic() < RecordBatch.MAGIC_VALUE_V2 && !records.isEmpty()
                ? records.getFirst().record().offset()
                : original.baseOffset();
        write(original, records, baseOffset, lastOffset, output);
    }

    /// As [#write(RecordBatch, List, long, ByteBufferOutputStream)], starting the batch at
    /// `baseOffset`; the base sequence moves along with it.
    private static void write(RecordBatch original, List<Entry> records, long baseOffset, long lastOffset, ByteBufferOutputStream output) {
        boolean legacy = original.magic() < RecordBatch.MAGIC_VALUE_V2;
        int baseSequence = original.baseSequence() == RecordBatch.NO_SEQUENCE || legacy
                ? original.baseSequence()
                : DefaultRecordBatch.incrementSequence(original.baseSequence(), (int) (baseOffset - original.baseOffset()));
        if (records.isEmpty()) {
            if (!legacy) {
                output.ensureRemaining(DefaultRecordBatch.RECORD_BATCH_OVERHEAD);
                DefaultRecordBatch.writeEmptyHeader(output.buffer(), RecordBatch.CURRENT_MAGIC_VALUE, original.producerId(),
                        original.producerEpoch(), baseSequence, baseOffset, lastOffset,
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
        try (var builder = new MemoryRecordsBuilder(output, RecordBatch.CURRENT_MAGIC_VALUE,
                Compression.of(original.compressionType()).build(), timestampType, baseOffset, logAppendTime,
                original.producerId(), original.producerEpoch(), baseSequence, original.isTransactional(),
                false, original.partitionLeaderEpoch(), output.limit(),
                original.deleteHorizonMs().orElse(RecordBatch.NO_TIMESTAMP))) {
            for (Entry entry : records) {
                Record record = entry.record();
                builder.appendWithOffset(record.offset(), record.timestamp(), record.key(), entry.value(), entry.headers());
            }
            if (!legacy) {
                builder.overrideLastOffset(lastOffset);
            }
            builder.build();
        }
    }
}

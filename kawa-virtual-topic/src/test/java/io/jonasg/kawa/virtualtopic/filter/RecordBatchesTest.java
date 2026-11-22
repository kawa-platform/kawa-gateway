package io.jonasg.kawa.virtualtopic.filter;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.internal.ControlRecordType;
import org.apache.kafka.common.record.internal.EndTransactionMarker;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MutableRecordBatch;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/// The records a consumer fetching at a position gets from the batches the broker returns, which
/// start at the batch holding that position.
class RecordBatchesTest {

    @Test
    void leavesOutTheRecordsBelowTheConsumersFetchOffset() {
        // given a batch at offsets 1-7, fetched at offset 5
        var records = MemoryRecords.withRecords(1L, Compression.NONE, values("r1", "r2", "r3", "r4", "r5", "r6", "r7"));

        // when
        MemoryRecords from = RecordBatches.from(records, 5L);

        // then
        assertThat(records(from)).extracting(Record::offset)
                .withFailMessage(() -> "Records below the fetch offset were delivered, or records from it on were lost")
                .containsExactly(5L, 6L, 7L);
        assertThat(records(from)).extracting(RecordBatchesTest::value)
                .withFailMessage(() -> "The records from the fetch offset on did not keep their values")
                .containsExactly("r5", "r6", "r7");
    }

    @Test
    void startsTheBatchHoldingTheFetchOffsetAtIt() {
        // given a batch at offsets 1-7, fetched at offset 5
        var records = MemoryRecords.withRecords(1L, Compression.NONE, values("r1", "r2", "r3", "r4", "r5", "r6", "r7"));

        // when
        MutableRecordBatch batch = batches(RecordBatches.from(records, 5L)).getFirst();

        // then a stop at the first record from the fetch offset leaves nothing before it to deliver
        assertThat(batch.baseOffset())
                .withFailMessage(() -> "The batch does not start at the fetch offset but at " + batch.baseOffset())
                .isEqualTo(5L);
        assertThat(batch.lastOffset())
                .withFailMessage(() -> "The batch does not end at its original last offset")
                .isEqualTo(7L);
    }

    @Test
    void leavesOutBatchesThatEndBelowTheConsumersFetchOffset() {
        // given batches at offsets 0-1 and 2-3, fetched at offset 2
        var records = concat(
                MemoryRecords.withRecords(Compression.NONE, values("r0", "r1")),
                MemoryRecords.withRecords(2L, Compression.NONE, values("r2", "r3")));

        // when
        MemoryRecords from = RecordBatches.from(records, 2L);

        // then
        assertThat(batches(from)).extracting(RecordBatch::baseOffset)
                .withFailMessage(() -> "A batch below the fetch offset was delivered")
                .containsExactly(2L);
        assertThat(records(from)).extracting(Record::offset)
                .withFailMessage(() -> "The records of the batch at the fetch offset were not delivered")
                .containsExactly(2L, 3L);
    }

    @Test
    void returnsTheRecordsUnchangedWhenTheyStartAtTheConsumersFetchOffset() {
        // given a batch at offsets 3-4, fetched at offset 3
        var records = MemoryRecords.withRecords(3L, Compression.NONE, values("r3", "r4"));

        // when / then
        assertThat(RecordBatches.from(records, 3L))
                .withFailMessage(() -> "Records starting at the fetch offset were rewritten")
                .isSameAs(records);
    }

    @Test
    void passesControlBatchesBelowTheConsumersFetchOffsetThroughUnchanged() {
        // given a transaction marker at offset 0 and a batch at offsets 1-2, fetched at offset 2
        var marker = MemoryRecords.withEndTransactionMarker(0L, 0L, 0, 7L, (short) 0,
                new EndTransactionMarker(ControlRecordType.COMMIT, 0));
        var records = concat(marker, MemoryRecords.withRecords(1L, Compression.NONE, values("r1", "r2")));

        // when
        List<MutableRecordBatch> batches = batches(RecordBatches.from(records, 2L));

        // then
        assertThat(batches.getFirst().isControlBatch())
                .withFailMessage(() -> "The transaction marker was not passed through")
                .isTrue();
        assertThat(batches.getFirst().lastOffset())
                .withFailMessage(() -> "The transaction marker was changed")
                .isZero();
    }

    private static SimpleRecord[] values(String... values) {
        SimpleRecord[] records = new SimpleRecord[values.length];
        for (int i = 0; i < values.length; i++) {
            records[i] = new SimpleRecord(values[i].getBytes(StandardCharsets.UTF_8));
        }
        return records;
    }

    private static String value(Record record) {
        return StandardCharsets.UTF_8.decode(record.value()).toString();
    }

    private static List<Record> records(MemoryRecords records) {
        List<Record> all = new ArrayList<>();
        records.records().forEach(all::add);
        return all;
    }

    private static List<MutableRecordBatch> batches(MemoryRecords records) {
        List<MutableRecordBatch> all = new ArrayList<>();
        records.batches().forEach(all::add);
        return all;
    }

    private static MemoryRecords concat(MemoryRecords... parts) {
        int size = 0;
        for (MemoryRecords part : parts) {
            size += part.sizeInBytes();
        }
        ByteBuffer buffer = ByteBuffer.allocate(size);
        for (MemoryRecords part : parts) {
            buffer.put(part.buffer().duplicate());
        }
        buffer.flip();
        return MemoryRecords.readableRecords(buffer);
    }
}

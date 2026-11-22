package io.jonasg.kawa.virtualtopic.filter;

import io.jonasg.kawa.config.AvroFormatConfig;
import io.jonasg.kawa.config.CelFilterConfig;
import io.jonasg.kawa.config.DecodeErrorPolicy;
import io.jonasg.kawa.config.JsonFormatConfig;
import io.jonasg.kawa.config.PayloadFormatConfig;
import io.jonasg.kawa.config.VirtualTopicFilterConfig;
import org.apache.avro.Schema;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.internal.BaseRecords;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/// Batch-level tests for [VirtualTopicRecordFilter#apply(BaseRecords)] - the decode-filter-reencode path
/// the fetch pipeline uses - with CEL filters over JSON record values.
class VirtualTopicRecordFilterTest {

    @Test
    void dropsRecordsThatDoNotMatchJsonFilter() {
        // given a batch with a matching and a non-matching JSON record
        var records = MemoryRecords.withRecords(Compression.NONE,
                record("{\"status\":\"PAID\"}"),
                record("{\"status\":\"OPEN\"}"));

        // when
        var filtered = (MemoryRecords) filter(
                new CelFilterConfig("value.status == \"PAID\""),
                new JsonFormatConfig(),
                records);

        // then
        List<Record> survivors = survivors(filtered);
        assertThat(survivors).hasSize(1);
        assertThat(value(survivors.get(0)))
                .withFailMessage(() -> "Non-matching JSON record was not dropped from the batch")
                .isEqualTo("{\"status\":\"PAID\"}");
    }

    @Test
    void keepsOriginalOffsetsOfSurvivingRecords() {
        // given a batch whose first record does not match the filter
        var records = MemoryRecords.withRecords(Compression.NONE,
                record("{\"status\":\"OPEN\"}"),
                record("{\"status\":\"PAID\"}"));

        // when
        var filtered = (MemoryRecords) filter(
                new CelFilterConfig("value.status == \"PAID\""),
                new JsonFormatConfig(),
                records);

        // then the surviving record keeps its original offset 1 instead of being renumbered to 0
        List<Record> survivors = survivors(filtered);
        assertThat(survivors).hasSize(1);
        assertThat(survivors.get(0).offset())
                .withFailMessage(() -> "Surviving record was renumbered instead of keeping its original offset")
                .isEqualTo(1L);
    }

    @Test
    void fullyFilteredBatchStillReturnsValidEmptyRecords() {
        // given a batch with no matching records
        var records = MemoryRecords.withRecords(Compression.NONE,
                record("{\"status\":\"OPEN\"}"),
                record("{\"status\":\"VOID\"}"));

        // when
        var filtered = (MemoryRecords) filter(
                new CelFilterConfig("value.status == \"PAID\""),
                new JsonFormatConfig(),
                records);

        // then the result is still a readable, valid records payload with no records
        List<Record> survivors = survivors(filtered);
        assertThat(survivors)
                .withFailMessage(() -> "Fully filtered batch did not yield a readable empty records payload")
                .isEmpty();
    }

    @Test
    void invalidJsonIsDroppedByDefaultSkipPolicy() {
        // given a batch with a matching record and a record that is not valid JSON
        var records = MemoryRecords.withRecords(Compression.NONE,
                record("{\"status\":\"PAID\"}"),
                record("not json"));

        // when
        var filtered = (MemoryRecords) filter(
                new CelFilterConfig("value.status == \"PAID\""),
                new JsonFormatConfig(),
                records);

        // then only the valid matching record survives
        List<Record> survivors = survivors(filtered);
        assertThat(survivors).hasSize(1);
        assertThat(value(survivors.get(0)))
                .withFailMessage(() -> "Invalid JSON record was not dropped with the default skip policy")
                .isEqualTo("{\"status\":\"PAID\"}");
    }

    @Test
    void invalidJsonIsDeliveredWithIncludePolicy() {
        // given a batch with a matching record and an invalid JSON record
        var records = MemoryRecords.withRecords(Compression.NONE,
                record("{\"status\":\"PAID\"}"),
                record("not json"));

        // when
        var filtered = (MemoryRecords) filter(
                new CelFilterConfig("value.status == \"PAID\""),
                new JsonFormatConfig(DecodeErrorPolicy.INCLUDE),
                records);

        // then the invalid record is delivered alongside the matching one, in order
        List<Record> survivors = survivors(filtered);
        assertThat(survivors).hasSize(2);
        assertThat(value(survivors.get(1)))
                .withFailMessage(() -> "Invalid JSON record was not delivered with the include policy")
                .isEqualTo("not json");
    }

    @Test
    void deliversTheMatchingRecordsBeforeAnInvalidJsonRecordWithFailPolicy() {
        // given a batch whose third record is not valid JSON, followed by a matching record
        var records = MemoryRecords.withRecords(Compression.NONE,
                record("{\"status\":\"PAID\"}"),
                record("{\"status\":\"OPEN\"}"),
                record("not json"),
                record("{\"status\":\"PAID\"}"));

        // when
        FailedRecordException failure = failOn(records);

        // then
        assertThat(survivors(failure.recordsBefore())).extracting(Record::offset)
                .withFailMessage(() -> "The matching record before the invalid one was not delivered, or a record after it was")
                .containsExactly(0L);
    }

    @Test
    void stopsThePartitionAtTheOffsetOfAnInvalidJsonRecordWithFailPolicy() {
        // given a batch whose third record is not valid JSON
        var records = MemoryRecords.withRecords(Compression.NONE,
                record("{\"status\":\"PAID\"}"),
                record("{\"status\":\"OPEN\"}"),
                record("not json"),
                record("{\"status\":\"PAID\"}"));

        // when
        FailedRecordException failure = failOn(records);

        // then
        assertThat(failure.offset())
                .withFailMessage(() -> "The partition was not stopped at the offset of the invalid record, but at " + failure.offset())
                .isEqualTo(2L);
    }

    @Test
    void endsTheDeliveredRecordsRightBeforeAnInvalidJsonRecordWithFailPolicy() {
        // given a batch whose third record is not valid JSON
        var records = MemoryRecords.withRecords(Compression.NONE,
                record("{\"status\":\"PAID\"}"),
                record("{\"status\":\"OPEN\"}"),
                record("not json"),
                record("{\"status\":\"PAID\"}"));

        // when
        FailedRecordException failure = failOn(records);

        // then the consumer's next fetch starts at the invalid record, not past it
        assertThat(lastOffset(failure.recordsBefore()))
                .withFailMessage(() -> "The delivered records do not end right before the invalid record, so the consumer would skip it")
                .isEqualTo(1L);
    }

    @Test
    void movesTheConsumerUpToAnInvalidJsonRecordWhenNoRecordBeforeItMatchesWithFailPolicy() {
        // given a batch whose only record before the invalid one does not match
        var records = MemoryRecords.withRecords(Compression.NONE,
                record("{\"status\":\"OPEN\"}"),
                record("not json"));

        // when
        FailedRecordException failure = failOn(records);

        // then
        assertThat(survivors(failure.recordsBefore()))
                .withFailMessage(() -> "A record that does not match the filter was delivered")
                .isEmpty();
        assertThat(lastOffset(failure.recordsBefore()))
                .withFailMessage(() -> "The consumer is not moved up to the invalid record")
                .isEqualTo(0L);
    }

    @Test
    void deliversNothingWhenTheFirstRecordIsInvalidJsonWithFailPolicy() {
        // given a batch whose first record is not valid JSON
        var records = MemoryRecords.withRecords(Compression.NONE,
                record("not json"),
                record("{\"status\":\"PAID\"}"));

        // when
        FailedRecordException failure = failOn(records);

        // then
        assertThat(failure.recordsBefore().sizeInBytes())
                .withFailMessage(() -> "Records were delivered although the invalid record comes first")
                .isZero();
        assertThat(failure.offset())
                .withFailMessage(() -> "The partition was not stopped at the first record")
                .isZero();
    }

    @Test
    void deliversTheBatchesBeforeTheBatchHoldingAnInvalidJsonRecordWithFailPolicy() {
        // given a batch at offsets 0-1 and a batch at offsets 2-3 that starts with an invalid record
        var records = concat(
                MemoryRecords.withRecords(Compression.NONE, record("{\"status\":\"PAID\"}"), record("{\"status\":\"PAID\"}")),
                MemoryRecords.withRecords(2L, Compression.NONE, record("not json"), record("{\"status\":\"PAID\"}")));

        // when
        FailedRecordException failure = failOn(records);

        // then
        assertThat(survivors(failure.recordsBefore())).extracting(Record::offset)
                .withFailMessage(() -> "The records of the batch before the invalid record were not all delivered")
                .containsExactly(0L, 1L);
        assertThat(failure.offset())
                .withFailMessage(() -> "The partition was not stopped at the invalid record of the second batch")
                .isEqualTo(2L);
    }

    @ParameterizedTest
    @EnumSource(DecodeErrorPolicy.class)
    void doesNotTreatAFilterThatCannotBeEvaluatedAsAnInvalidRecordWhateverThePolicy(DecodeErrorPolicy policy) {
        // given a filter that fails to evaluate on a valid JSON record
        var records = MemoryRecords.withRecords(Compression.NONE, record("{\"n\":\"not a number\"}"));

        // when / then the record is neither dropped, delivered, nor reported as an invalid record
        assertThatThrownBy(() -> filter(
                new CelFilterConfig("int(value.n) == 1"),
                new JsonFormatConfig(policy),
                records))
                .withFailMessage(() -> "A filter evaluation error was handled by the " + policy + " policy as if the record were invalid")
                .isNotInstanceOf(FailedRecordException.class);
    }

    private FailedRecordException failOn(MemoryRecords records) {
        FailedRecordException failure = catchThrowableOfType(FailedRecordException.class, () -> filter(
                new CelFilterConfig("value.status == \"PAID\""),
                new JsonFormatConfig(DecodeErrorPolicy.FAIL),
                records));
        assertThat(failure)
                .withFailMessage(() -> "The invalid record did not stop the partition under the fail policy")
                .isNotNull();
        return failure;
    }

    private static long lastOffset(MemoryRecords records) {
        long last = -1L;
        for (var batch : records.batches()) {
            last = batch.lastOffset();
        }
        return last;
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

    @Test
    void tombstoneIsDroppedByDefault() {
        // given a batch with a tombstone (null value) and a matching record
        var records = MemoryRecords.withRecords(Compression.NONE,
                tombstone(),
                record("{\"status\":\"PAID\"}"));

        // when
        var filtered = (MemoryRecords) filter(
                new CelFilterConfig("value.status == \"PAID\""),
                new JsonFormatConfig(),
                records);

        // then the tombstone is dropped and only the matching record survives
        List<Record> survivors = survivors(filtered);
        assertThat(survivors).hasSize(1);
        assertThat(value(survivors.get(0)))
                .withFailMessage(() -> "Tombstone was not dropped with the default skip policy")
                .isEqualTo("{\"status\":\"PAID\"}");
    }

    @Test
    void rawStringValuesFilterWithoutValueFormat() {
        // given no value format - the filter runs against the raw value string
        var records = MemoryRecords.withRecords(Compression.NONE,
                record("{\"status\":\"PAID\"}"),
                record("{\"status\":\"OPEN\"}"));

        // when
        var filtered = (MemoryRecords) filter(
                new CelFilterConfig("value.contains(\"PAID\")"),
                null,
                records);

        // then
        List<Record> survivors = survivors(filtered);
        assertThat(survivors).hasSize(1);
        assertThat(value(survivors.get(0)))
                .withFailMessage(() -> "Raw string value containing PAID was not matched without a value format")
                .isEqualTo("{\"status\":\"PAID\"}");
    }

    @ParameterizedTest
    @EnumSource(DecodeErrorPolicy.class)
    void failsTheBatchWhenTheRegistryIsUnavailableWhateverThePolicy(DecodeErrorPolicy policy) {
        // given an Avro topic whose registry is down
        var outage = new SchemaRegistryUnavailableException("schema registry is unreachable");
        AvroSchemaResolver unavailable = id -> {
            throw outage;
        };
        Schema string = Schema.create(Schema.Type.STRING);
        var records = MemoryRecords.withRecords(Compression.NONE,
                new SimpleRecord(1000L, "k".getBytes(StandardCharsets.UTF_8), AvroRecordPredicateTest.wireFormat(9, string, "PAID")));

        // when / then the record is neither dropped nor delivered unfiltered
        assertThatThrownBy(() -> VirtualTopicRecordFilter.of("orders", new CelFilterConfig("value == \"PAID\""), new AvroFormatConfig(policy), unavailable)
                .apply(records))
                .withFailMessage(() -> "A registry outage was handled by the " + policy + " policy instead of failing the batch")
                .isSameAs(outage);
    }

    private static SimpleRecord record(String value) {
        return new SimpleRecord(1000L, "k".getBytes(StandardCharsets.UTF_8),
                value.getBytes(StandardCharsets.UTF_8));
    }

    private static SimpleRecord tombstone() {
        return new SimpleRecord(1000L, "k".getBytes(StandardCharsets.UTF_8), null);
    }

    private static String value(Record record) {
        return StandardCharsets.UTF_8.decode(record.value()).toString();
    }

    private static List<Record> survivors(MemoryRecords records) {
        List<Record> survivors = new ArrayList<>();
        records.records().forEach(survivors::add);
        return survivors;
    }

    private static BaseRecords filter(VirtualTopicFilterConfig filter, PayloadFormatConfig valueFormat, BaseRecords records) {
        return VirtualTopicRecordFilter.of("orders", filter, valueFormat, null).apply(records);
    }
}

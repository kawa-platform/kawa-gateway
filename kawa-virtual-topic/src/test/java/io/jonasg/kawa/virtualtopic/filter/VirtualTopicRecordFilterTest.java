package io.jonasg.kawa.virtualtopic.filter;

import io.jonasg.kawa.config.CelFilterConfig;
import io.jonasg.kawa.config.DecodeErrorPolicy;
import io.jonasg.kawa.config.JsonFormatConfig;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Batch-level tests for [VirtualTopicRecordFilter#apply] - the decode-filter-reencode path
/// the fetch pipeline uses - with CEL filters over JSON record values.
class VirtualTopicRecordFilterTest {

    private final VirtualTopicRecordFilter filter = new VirtualTopicRecordFilter();

    @Test
    void dropsRecordsThatDoNotMatchJsonFilter() {
        // given a batch with a matching and a non-matching JSON record
        var records = MemoryRecords.withRecords(Compression.NONE,
                record("{\"status\":\"PAID\"}"),
                record("{\"status\":\"OPEN\"}"));

        // when
        var filtered = (MemoryRecords) filter.apply(
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
        var filtered = (MemoryRecords) filter.apply(
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
        var filtered = (MemoryRecords) filter.apply(
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
        var filtered = (MemoryRecords) filter.apply(
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
        var filtered = (MemoryRecords) filter.apply(
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
    void invalidJsonFailsWithFailPolicy() {
        // given a batch containing an invalid JSON record
        var records = MemoryRecords.withRecords(Compression.NONE,
                record("{\"status\":\"PAID\"}"),
                record("not json"));

        // when / then
        assertThatThrownBy(() -> filter.apply(
                new CelFilterConfig("value.status == \"PAID\""),
                new JsonFormatConfig(DecodeErrorPolicy.FAIL),
                records))
                .withFailMessage(() -> "Invalid JSON record did not fail the batch with the fail policy")
                .isInstanceOf(PayloadDecodeException.class);
    }

    @Test
    void tombstoneIsDroppedByDefault() {
        // given a batch with a tombstone (null value) and a matching record
        var records = MemoryRecords.withRecords(Compression.NONE,
                tombstone(),
                record("{\"status\":\"PAID\"}"));

        // when
        var filtered = (MemoryRecords) filter.apply(
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
        var filtered = (MemoryRecords) filter.apply(
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
}

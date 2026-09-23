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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// CEL filters over JSON record values, via a virtual topic's `valueFormat: {type: json}`.
class JsonRecordPredicateTest {

    @Test
    void matchesOnJsonField() {
        // given
        var filter = jsonFilter("value.status == \"PAID\"");

        // when / then
        assertThat(filter.matches(record("{\"status\":\"PAID\"}")))
                .withFailMessage(() -> "Record with status PAID was not matched")
                .isTrue();
        assertThat(filter.matches(record("{\"status\":\"OPEN\"}")))
                .withFailMessage(() -> "Record with status OPEN was not rejected")
                .isFalse();
    }

    @Test
    void matchesOnNestedField() {
        // given
        var filter = jsonFilter("value.customer.country == \"BE\"");

        // when
        var matches = filter.matches(record("{\"customer\":{\"id\":7,\"country\":\"BE\"}}"));

        // then
        assertThat(matches)
                .withFailMessage(() -> "Record with nested customer.country BE was not matched")
                .isTrue();
    }

    @Test
    void comparesIntegerAndDecimalNumbersAgainstIntLiteral() {
        // given
        var filter = jsonFilter("value.amount > 100");

        // when / then
        assertThat(filter.matches(record("{\"amount\":150}")))
                .withFailMessage(() -> "Integer amount 150 was not matched by > 100")
                .isTrue();
        assertThat(filter.matches(record("{\"amount\":100.5}")))
                .withFailMessage(() -> "Decimal amount 100.5 was not matched by > 100")
                .isTrue();
        assertThat(filter.matches(record("{\"amount\":99}")))
                .withFailMessage(() -> "Amount 99 was not rejected by > 100")
                .isFalse();
    }

    @Test
    void matchesOnArrayContents() {
        // given
        var filter = jsonFilter("\"urgent\" in value.tags");

        // when / then
        assertThat(filter.matches(record("{\"tags\":[\"urgent\",\"eu\"]}")))
                .withFailMessage(() -> "Record tagged urgent was not matched")
                .isTrue();
        assertThat(filter.matches(record("{\"tags\":[\"eu\"]}")))
                .withFailMessage(() -> "Record without urgent tag was not rejected")
                .isFalse();
    }

    @Test
    void missingFieldDoesNotMatchInsteadOfFailing() {
        // given
        var filter = jsonFilter("value.status == \"PAID\"");

        // when
        var matches = filter.matches(record("{\"amount\":10}"));

        // then
        assertThat(matches)
                .withFailMessage(() -> "Record without a status field was not rejected")
                .isFalse();
    }

    @Test
    void presenceCanBeTestedWithHas() {
        // given
        var filter = jsonFilter("has(value.refundId)");

        // when / then
        assertThat(filter.matches(record("{\"refundId\":\"r-1\"}")))
                .withFailMessage(() -> "Record with refundId was not matched by has()")
                .isTrue();
        assertThat(filter.matches(record("{\"orderId\":\"o-1\"}")))
                .withFailMessage(() -> "Record without refundId was not rejected by has()")
                .isFalse();
    }

    @Test
    void jsonNullFieldEqualsNull() {
        // given
        var filter = jsonFilter("value.cancelledAt == null");

        // when
        var matches = filter.matches(record("{\"cancelledAt\":null}"));

        // then
        assertThat(matches)
                .withFailMessage(() -> "JSON null field was not equal to CEL null")
                .isTrue();
    }

    @Test
    void invalidJsonIsDroppedByDefault() {
        // given
        var filter = jsonFilter("value.status == \"PAID\"");

        // when
        var matches = filter.matches(record("not json"));

        // then
        assertThat(matches)
                .withFailMessage(() -> "Invalid JSON record was not dropped with the default skip policy")
                .isFalse();
    }

    @Test
    void invalidJsonIsDeliveredWithIncludePolicy() {
        // given
        var filter = jsonFilter("value.status == \"PAID\"", DecodeErrorPolicy.INCLUDE);

        // when
        var matches = filter.matches(record("not json"));

        // then
        assertThat(matches)
                .withFailMessage(() -> "Invalid JSON record was not delivered with the include policy")
                .isTrue();
    }

    @Test
    void invalidJsonFailsWithFailPolicy() {
        // given
        var filter = jsonFilter("value.status == \"PAID\"", DecodeErrorPolicy.FAIL);

        // when / then
        assertThatThrownBy(() -> filter.matches(record("not json")))
                .withFailMessage(() -> "Invalid JSON record did not fail the filter with the fail policy")
                .isInstanceOf(PayloadDecodeException.class);
    }

    @Test
    void emptyValueIsTreatedAsDecodeError() {
        // given a tombstone and a filter that delivers undecodable records
        var filter = jsonFilter("value.status == \"PAID\"", DecodeErrorPolicy.INCLUDE);
        var tombstone = record(new SimpleRecord(1000L, "k".getBytes(StandardCharsets.UTF_8), null));

        // when
        var matches = filter.matches(tombstone);

        // then
        assertThat(matches)
                .withFailMessage(() -> "Tombstone was not handled by the decode error policy")
                .isTrue();
    }

    @Test
    void rawStringValueStillWorksWithoutFormat() {
        // given no value format - value is bound as the raw string
        var filter = new VirtualTopicRecordFilter.EvaluatingRecordFilter(
                new CelFilterConfig("value.contains(\"PAID\")"));

        // when
        var matches = filter.matches(record("{\"status\":\"PAID\"}"));

        // then
        assertThat(matches)
                .withFailMessage(() -> "Raw string value was not matched when no format is configured")
                .isTrue();
    }

    @Test
    void fieldAccessWithoutFormatIsRejectedWhenFilterIsCreated() {
        // given a JSON-style expression but no value format - value is a string
        var config = new CelFilterConfig("value.status == \"PAID\"");

        // when / then
        assertThatThrownBy(() -> new VirtualTopicRecordFilter.EvaluatingRecordFilter(config))
                .withFailMessage(() -> "Field access on a raw string value was not rejected when the filter was created")
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static VirtualTopicRecordFilter.EvaluatingRecordFilter jsonFilter(String expression) {
        return new VirtualTopicRecordFilter.EvaluatingRecordFilter(
                new CelFilterConfig(expression), new JsonFormatConfig());
    }

    private static VirtualTopicRecordFilter.EvaluatingRecordFilter jsonFilter(String expression, DecodeErrorPolicy onDecodeError) {
        return new VirtualTopicRecordFilter.EvaluatingRecordFilter(
                new CelFilterConfig(expression), new JsonFormatConfig(onDecodeError));
    }

    private static Record record(String value) {
        return record(new SimpleRecord(
                1000L, "k".getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8)));
    }

    /// Wraps a [SimpleRecord] in a [MemoryRecords] batch and returns the decoded [Record],
    /// matching how records reach the evaluator in the real fetch pipeline.
    private static Record record(SimpleRecord simple) {
        return MemoryRecords.withRecords(Compression.NONE, simple).records().iterator().next();
    }
}

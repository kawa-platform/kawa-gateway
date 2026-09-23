package io.jonasg.kawa.virtualtopic.filter;

import io.jonasg.kawa.config.CelFilterConfig;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CelRecordPredicateTest {

    @Test
    void celExpressionMatchesHeader() {
        // given a CEL filter for tenant=acme
        var filter = new VirtualTopicRecordFilter.EvaluatingRecordFilter(
                new CelFilterConfig("headers.tenant == \"acme\""));
        var record = record(new SimpleRecord(
                1000L, "k1".getBytes(StandardCharsets.UTF_8), "v1".getBytes(StandardCharsets.UTF_8),
                new Header[]{new RecordHeader("tenant", "acme".getBytes(StandardCharsets.UTF_8))}));

        // when
        var matches = filter.matches(record);

        // then
        assertThat(matches).isTrue();
    }

    @Test
    void celExpressionRejectsNonMatchingHeader() {
        // given a CEL filter for tenant=acme
        var filter = new VirtualTopicRecordFilter.EvaluatingRecordFilter(
                new CelFilterConfig("headers.tenant == \"acme\""));
        var record = record(new SimpleRecord(
                1000L, "k1".getBytes(StandardCharsets.UTF_8), "v1".getBytes(StandardCharsets.UTF_8),
                new Header[]{new RecordHeader("tenant", "other".getBytes(StandardCharsets.UTF_8))}));

        // when
        var matches = filter.matches(record);

        // then
        assertThat(matches).isFalse();
    }

    @Test
    void celExpressionMatchesKeyPrefix() {
        // given a CEL filter matching keys starting with "order-"
        var filter = new VirtualTopicRecordFilter.EvaluatingRecordFilter(
                new CelFilterConfig("key.startsWith(\"order-\")"));
        var matching = record(new SimpleRecord(
                1000L, "order-123".getBytes(StandardCharsets.UTF_8), "v".getBytes(StandardCharsets.UTF_8)));
        var nonMatching = record(new SimpleRecord(
                1000L, "event-456".getBytes(StandardCharsets.UTF_8), "v".getBytes(StandardCharsets.UTF_8)));

        // when / then
        assertThat(filter.matches(matching)).isTrue();
        assertThat(filter.matches(nonMatching)).isFalse();
    }

    @Test
    void celExpressionWithBooleanConjunction() {
        // given a CEL filter requiring both tenant=acme AND env=prod
        var filter = new VirtualTopicRecordFilter.EvaluatingRecordFilter(
                new CelFilterConfig("headers.tenant == \"acme\" && headers.env == \"prod\""));
        var matching = record(new SimpleRecord(
                1000L, "k".getBytes(StandardCharsets.UTF_8), "v".getBytes(StandardCharsets.UTF_8),
                new Header[]{
                        new RecordHeader("tenant", "acme".getBytes(StandardCharsets.UTF_8)),
                        new RecordHeader("env", "prod".getBytes(StandardCharsets.UTF_8))
                }));
        var partial = record(new SimpleRecord(
                1000L, "k".getBytes(StandardCharsets.UTF_8), "v".getBytes(StandardCharsets.UTF_8),
                new Header[]{new RecordHeader("tenant", "acme".getBytes(StandardCharsets.UTF_8))}));

        // when / then
        assertThat(filter.matches(matching)).isTrue();
        assertThat(filter.matches(partial)).isFalse();
    }

    @Test
    void celExpressionHandlesNullKey() {
        // given a CEL filter matching on value
        var filter = new VirtualTopicRecordFilter.EvaluatingRecordFilter(new CelFilterConfig("value == \"hello\""));
        var record = record(new SimpleRecord(1000L, null, "hello".getBytes(StandardCharsets.UTF_8)));

        // when
        var matches = filter.matches(record);

        // then
        assertThat(matches).isTrue();
    }

    @Test
    void celExpressionHandlesNullValue() {
        // given a CEL filter matching on key
        var filter = new VirtualTopicRecordFilter.EvaluatingRecordFilter(new CelFilterConfig("key == \"test\""));
        var record = record(new SimpleRecord(1000L, "test".getBytes(StandardCharsets.UTF_8), null));

        // when
        var matches = filter.matches(record);

        // then
        assertThat(matches).isTrue();
    }

    @Test
    void celExpressionMatchesValueContains() {
        // given a CEL filter checking if value contains "error"
        var filter = new VirtualTopicRecordFilter.EvaluatingRecordFilter(
                new CelFilterConfig("value.contains(\"error\")"));
        var matching = record(new SimpleRecord(
                1000L, "k".getBytes(StandardCharsets.UTF_8), "error in request".getBytes(StandardCharsets.UTF_8)));
        var nonMatching = record(new SimpleRecord(
                1000L, "k".getBytes(StandardCharsets.UTF_8), "success".getBytes(StandardCharsets.UTF_8)));

        // when / then
        assertThat(filter.matches(matching)).isTrue();
        assertThat(filter.matches(nonMatching)).isFalse();
    }

    @Test
    void celExpressionMatchesTimestampComparison() {
        // given a CEL filter for timestamp > 2000
        var filter = new VirtualTopicRecordFilter.EvaluatingRecordFilter(new CelFilterConfig("timestamp > 2000"));
        var after = record(new SimpleRecord(
                3000L, "k".getBytes(StandardCharsets.UTF_8), "v".getBytes(StandardCharsets.UTF_8)));
        var before = record(new SimpleRecord(
                1000L, "k".getBytes(StandardCharsets.UTF_8), "v".getBytes(StandardCharsets.UTF_8)));

        // when / then
        assertThat(filter.matches(after)).isTrue();
        assertThat(filter.matches(before)).isFalse();
    }

    @Test
    void celExpressionRejectsRecordWhenHeaderMissing() {
        // given a CEL filter for tenant=acme
        var filter = new VirtualTopicRecordFilter.EvaluatingRecordFilter(
                new CelFilterConfig("headers.tenant == \"acme\""));
        var record = record(new SimpleRecord(
                1000L, "k".getBytes(StandardCharsets.UTF_8), "v".getBytes(StandardCharsets.UTF_8)));

        // when — record has no headers at all
        var matches = filter.matches(record);

        // then — missing header resolves to "", which != "acme"
        assertThat(matches).isFalse();
    }

    @Test
    void invalidCelExpressionIsRejectedWhenFilterIsCreated() {
        // given a syntactically invalid CEL expression
        var config = new CelFilterConfig("headers.tenant ==");

        // when / then compilation happens once, up front, so the error surfaces at construction
        // rather than on (and repeated for) every evaluated record
        assertThatThrownBy(() -> new VirtualTopicRecordFilter.EvaluatingRecordFilter(config))
                .withFailMessage(() -> "Invalid CEL expression was not rejected when the filter was created")
                .isInstanceOf(IllegalArgumentException.class);
    }

    /// Wraps a [SimpleRecord] in a [MemoryRecords] batch and returns the decoded [Record],
    /// matching how records reach the evaluator in the real fetch pipeline.
    private static Record record(SimpleRecord simple) {
        return MemoryRecords.withRecords(Compression.NONE, simple).records().iterator().next();
    }
}

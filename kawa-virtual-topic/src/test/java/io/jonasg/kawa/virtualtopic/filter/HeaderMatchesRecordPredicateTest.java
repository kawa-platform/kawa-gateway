package io.jonasg.kawa.virtualtopic.filter;

import io.jonasg.kawa.config.HeaderMatchesFilterConfig;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class HeaderMatchesRecordPredicateTest {

    @Test
    void headerMatchesMatchesRecordWithMatchingHeader() {
        // given a headerMatches filter for tenant matching "eu.*"
        var filter = new VirtualTopicRecordFilter.EvaluatingRecordFilter(
                new HeaderMatchesFilterConfig("tenant", "eu.*"));
        var record = record(new SimpleRecord(
                1000L, "k".getBytes(StandardCharsets.UTF_8), "v".getBytes(StandardCharsets.UTF_8),
                new Header[]{new RecordHeader("tenant", "eu-west-1".getBytes(StandardCharsets.UTF_8))}));

        // when
        var matches = matches(filter, record);

        // then
        assertThat(matches).isTrue();
    }

    @Test
    void headerMatchesRejectsRecordWithPartiallyMatchingHeader() {
        // given a headerMatches filter for tenant matching "west" (full match required)
        var filter = new VirtualTopicRecordFilter.EvaluatingRecordFilter(
                new HeaderMatchesFilterConfig("tenant", "west"));
        var record = record(new SimpleRecord(
                1000L, "k".getBytes(StandardCharsets.UTF_8), "v".getBytes(StandardCharsets.UTF_8),
                new Header[]{new RecordHeader("tenant", "eu-west-1".getBytes(StandardCharsets.UTF_8))}));

        // when
        var matches = matches(filter, record);

        // then
        assertThat(matches).isFalse();
    }

    @Test
    void headerMatchesRejectsRecordWithMissingHeader() {
        // given a headerMatches filter for tenant matching "eu.*"
        var filter = new VirtualTopicRecordFilter.EvaluatingRecordFilter(
                new HeaderMatchesFilterConfig("tenant", "eu.*"));
        var record = record(new SimpleRecord(
                1000L, "k".getBytes(StandardCharsets.UTF_8), "v".getBytes(StandardCharsets.UTF_8),
                new Header[]{new RecordHeader("region", "eu".getBytes(StandardCharsets.UTF_8))}));

        // when
        var matches = matches(filter, record);

        // then
        assertThat(matches).isFalse();
    }

    /// Wraps a [SimpleRecord] in a [MemoryRecords] batch and returns the decoded [Record],
    /// matching how records reach the evaluator in the real fetch pipeline.
    private static Record record(SimpleRecord simple) {
        return MemoryRecords.withRecords(Compression.NONE, simple).records().iterator().next();
    }

    /// Evaluates `filter` on `record`, whose value the filter reads as stored.
    private static boolean matches(VirtualTopicRecordFilter.EvaluatingRecordFilter filter, Record record) {
        return filter.matches(record, new DecodedValue(record, null));
    }
}

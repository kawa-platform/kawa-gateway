package io.jonasg.kawa.virtualtopic.filter;

import io.jonasg.kawa.config.CelFilterConfig;
import io.jonasg.kawa.config.XmlFormatConfig;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/// CEL filters over XML record values, via a virtual topic's `valueFormat: {type: xml}`.
class XmlRecordPredicateTest {

    @Test
    void matchesOnElementsAndAttributes() {
        // given
        var filter = xmlFilter("value.status == \"PAID\" && value[\"@region\"] == \"eu\" && int(value.qty) > 2");

        // when / then
        assertThat(filter.matches(record("<order region=\"eu\"><status>PAID</status><qty>3</qty></order>")))
                .withFailMessage(() -> "A paid EU order of 3 was not matched")
                .isTrue();
        assertThat(filter.matches(record("<order region=\"us\"><status>PAID</status><qty>3</qty></order>")))
                .withFailMessage(() -> "A US order was not rejected")
                .isFalse();
    }

    @Test
    void dropsAnUndecodableRecordByDefault() {
        // given
        var filter = xmlFilter("value.status == \"PAID\"");

        // then
        assertThat(filter.matches(record("<order>mixed<status>PAID</status></order>")))
                .withFailMessage(() -> "A record with mixed content was not dropped")
                .isFalse();
    }

    private static VirtualTopicRecordFilter.EvaluatingRecordFilter xmlFilter(String expression) {
        return new VirtualTopicRecordFilter.EvaluatingRecordFilter(new CelFilterConfig(expression), new XmlFormatConfig());
    }

    private static Record record(String value) {
        var simple = new SimpleRecord(1000L, "k".getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
        return MemoryRecords.withRecords(Compression.NONE, simple).records().iterator().next();
    }
}

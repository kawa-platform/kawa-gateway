package io.jonasg.kawa.mcp.peekrecords;

import io.jonasg.kawa.config.HeaderEqualsFilterConfig;
import io.jonasg.kawa.config.JsonFormatConfig;
import io.jonasg.kawa.config.VirtualTopicConfig;
import io.jonasg.kawa.governance.TopicDescriber.TopicState;
import io.jonasg.kawa.mcp.McpSliceTestBase;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import io.jonasg.kawa.virtualtopic.filter.SchemaRegistries;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.Map;

import static io.jonasg.kawa.test.KawaAssertions.assertThat;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;

class PeekRecordsMcpToolSliceTest extends McpSliceTestBase {

    private static final String TOPIC = "shipments";
    private static final Instant T0 = Instant.parse("2026-11-25T10:00:00Z");

    @Nested
    class LimitsAndPartitions {

        @Test
        void inspectsFiftyRecordsByDefault() throws Exception {
            // given
            topicWithPartitions(1);
            appendJson(0, 60);
            startServer();

            // when
            var result = peek("{\"topic\":\"shipments\"}");

            // then
            assertThatJson(result).inPath("limit").withFailMessage(() -> "the default limit is not 50: " + result).isEqualTo(50);
            assertThatJson(result).inPath("recordsInspected").withFailMessage(() -> "not 50 records were inspected: " + result)
                    .isEqualTo(50);
            assertThatJson(result).inPath("limitReached").withFailMessage(() -> "the limit was not reported reached: " + result)
                    .isEqualTo(true);
        }

        @Test
        void inspectsUpToFiveHundredRecords() throws Exception {
            // given
            topicWithPartitions(1);
            appendJson(0, 600);
            startServer();

            // when
            var result = peek("{\"topic\":\"shipments\",\"limit\":500}");

            // then
            assertThatJson(result).inPath("recordsInspected").withFailMessage(() -> "not 500 records were inspected")
                    .isEqualTo(500);
        }

        @ParameterizedTest
        @ValueSource(strings = {"501", "0", "-1", "1.5", "\"ten\""})
        void rejectsALimitOutsideOneToFiveHundred(String limit) throws Exception {
            // given
            topicWithPartitions(1);
            startServer();

            // when
            var error = peekError("{\"topic\":\"shipments\",\"limit\":" + limit + "}");

            // then
            assertThat(error).withFailMessage(() -> "limit " + limit + " was not refused naming limit: " + error).contains("limit");
        }

        @Test
        void sharesTheLimitOverAllSelectedPartitions() throws Exception {
            // given
            topicWithPartitions(3);
            appendJson(0, 20);
            appendJson(1, 20);
            appendJson(2, 20);
            startServer();

            // when
            var result = peek("{\"topic\":\"shipments\",\"limit\":12}");

            // then
            assertThatJson(result).inPath("recordsInspected").withFailMessage(() -> "the limit was applied per partition: " + result)
                    .isEqualTo(12);
            assertThatJson(result).inPath("ranges[*].recordsInspected").isArray()
                    .withFailMessage(() -> "the limit was not shared evenly: " + result).containsExactly(4, 4, 4);
        }

        @Test
        void givesQuietPartitionsTheirRecordsWhenOneIsBusy() throws Exception {
            // given
            topicWithPartitions(3);
            appendJson(0, 100);
            appendJson(1, 3);
            appendJson(2, 3);
            startServer();

            // when
            var result = peek("{\"topic\":\"shipments\",\"limit\":30}");

            // then
            assertThatJson(result).inPath("ranges[*].recordsInspected").isArray()
                    .withFailMessage(() -> "the busy partition crowded out the others: " + result).containsExactly(24, 3, 3);
        }

        @Test
        void rejectsAPartitionTheTopicDoesNotHave() throws Exception {
            // given
            topicWithPartitions(3);
            startServer();

            // when
            var error = peekError("{\"topic\":\"shipments\",\"partitions\":[1,7]}");

            // then
            assertThat(error).withFailMessage(() -> "partition 7 was not refused: " + error)
                    .contains("no partition(s) [7]").contains("0 to 2");
        }

        @Test
        void reportsAnEmptyPartitionAsFullyInspected() throws Exception {
            // given
            topicWithPartitions(2);
            appendJson(0, 2);
            startServer();

            // when
            var result = peek("{\"topic\":\"shipments\"}");

            // then
            assertThatJson(result).inPath("ranges[1]").withFailMessage(() -> "the empty partition is not reported complete")
                    .isEqualTo("{\"partition\":1,\"logStartOffset\":0,\"logEndOffset\":0,\"rangeStartOffset\":0,"
                            + "\"rangeEndOffset\":0,\"offsetsInRange\":0,\"inspectedFromOffset\":0,\"inspectedToOffset\":0,"
                            + "\"recordsInspected\":0,\"complete\":true}");
            assertThatJson(result).inPath("exhaustive").withFailMessage(() -> "two records on the topic were not exhaustive")
                    .isEqualTo(true);
        }

        @Test
        void returnsNullKeysAndValues() throws Exception {
            // given
            topicWithPartitions(1);
            records.append(TOPIC, 0, T0, (String) null, null);
            startServer();

            // when
            var result = peek("{\"topic\":\"shipments\",\"include_values\":true}");

            // then
            assertThatJson(result).inPath("records[0]").withFailMessage(() -> "a null key and value were not reported as such: " + result)
                    .isEqualTo("{\"topic\":\"shipments\",\"partition\":0,\"offset\":0,\"timestamp\":\"2026-11-25T10:00:00Z\","
                            + "\"timestampType\":\"CreateTime\",\"valueStatus\":\"null\"}");
        }
    }

    @Nested
    class Positioning {

        @Test
        void readsTheLatestRecordsByDefault() throws Exception {
            // given
            topicWithPartitions(1);
            appendJson(0, 100);
            startServer();

            // when
            var result = peek("{\"topic\":\"shipments\",\"limit\":5}");

            // then
            assertThatJson(result).inPath("records[*].offset").isArray()
                    .withFailMessage(() -> "the latest records were not returned: " + result).containsExactly(95, 96, 97, 98, 99);
            assertThatJson(result).inPath("position").isEqualTo("latest");
            assertThatJson(result).inPath("exhaustive").withFailMessage(() -> "5 of 100 records were exhaustive").isEqualTo(false);
            assertThatJson(result).inPath("summary").isString().contains("the latest ones").contains("Not exhaustive");
        }

        @Test
        void startsAtTheRequestedOffset() throws Exception {
            // given
            topicWithPartitions(3);
            appendJson(2, 100);
            startServer();

            // when
            var result = peek("{\"topic\":\"shipments\",\"partitions\":[2],\"start_offset\":40,\"limit\":3}");

            // then
            assertThatJson(result).inPath("records[*].offset").isArray()
                    .withFailMessage(() -> "inspection did not start at offset 40: " + result).containsExactly(40, 41, 42);
            assertThatJson(result).inPath("ranges[0].requestedStartOffset").isEqualTo(40);
            assertThatJson(result).inPath("ranges[0].rangeStartOffset").isEqualTo(40);
            assertThatJson(result).inPath("ranges[0].inspectedToOffset").isEqualTo(43);
        }

        @Test
        void startsEachPartitionAtItsOwnOffset() throws Exception {
            // given
            topicWithPartitions(3);
            appendJson(0, 20);
            appendJson(1, 20);
            appendJson(2, 20);
            startServer();

            // when
            var result = peek("{\"topic\":\"shipments\",\"start_offsets\":{\"0\":12,\"2\":5},\"limit\":4}");

            // then
            assertThatJson(result).inPath("partitions").withFailMessage(() -> "start_offsets did not select its partitions")
                    .isEqualTo("[0,2]");
            assertThatJson(result).inPath("records[?(@.partition==0)].offset").isArray().containsExactly(12, 13);
            assertThatJson(result).inPath("records[?(@.partition==2)].offset").isArray().containsExactly(5, 6);
        }

        @Test
        void startsAtTheFirstRecordAtOrAfterStartTime() throws Exception {
            // given
            topicWithPartitions(2);
            appendJson(0, 10);
            appendJson(1, 10);
            startServer();

            // when
            var result = peek("{\"topic\":\"shipments\",\"start_time\":\"2026-11-25T11:07:00+01:00\",\"limit\":4}");

            // then
            assertThatJson(result).inPath("position").isEqualTo("time");
            assertThatJson(result).inPath("startTime").isEqualTo("2026-11-25T10:07:00Z");
            assertThatJson(result).inPath("ranges[*].rangeStartOffset").isArray()
                    .withFailMessage(() -> "start_time did not resolve to offset 7 per partition: " + result).containsExactly(7, 7);
            assertThatJson(result).inPath("ranges[0].notes[0]").isString().contains("start_time resolved to offset 7");
            assertThatJson(result).inPath("records[*].offset").isArray().containsExactly(7, 7, 8, 8);
        }

        @Test
        void inspectsAnOffsetRangeUpToItsExclusiveEnd() throws Exception {
            // given
            topicWithPartitions(1);
            appendJson(0, 300);
            startServer();

            // when
            var result = peek("{\"topic\":\"shipments\",\"start_offset\":100,\"end_offset\":105,\"limit\":100}");

            // then
            assertThatJson(result).inPath("records[*].offset").isArray()
                    .withFailMessage(() -> "the range end was not exclusive: " + result).containsExactly(100, 101, 102, 103, 104);
            assertThatJson(result).inPath("exhaustive").withFailMessage(() -> "the whole range was not reported inspected")
                    .isEqualTo(true);
            assertThatJson(result).inPath("limitReached").isEqualTo(false);
        }

        @Test
        void inspectsATimeRange() throws Exception {
            // given
            topicWithPartitions(1);
            appendJson(0, 60);
            startServer();

            // when
            var result = peek("{\"topic\":\"shipments\",\"start_time\":\"2026-11-25T10:10:00Z\","
                    + "\"until\":\"2026-11-25T10:13:00Z\",\"limit\":500}");

            // then
            assertThatJson(result).inPath("records[*].offset").isArray()
                    .withFailMessage(() -> "the time range was not respected: " + result).containsExactly(10, 11, 12);
            assertThatJson(result).inPath("ranges[0].offsetsInRange").isEqualTo(3);
            assertThatJson(result).inPath("exhaustive").isEqualTo(true);
        }

        @Test
        void leavesOutRecordsWhoseCreateTimeLiesOutsideTheTimeRange() throws Exception {
            // given
            topicWithPartitions(1);
            records.append(TOPIC, 0, T0, "a", "1")
                    .append(TOPIC, 0, T0.plusSeconds(600), "b", "2")
                    .append(TOPIC, 0, T0.minusSeconds(600), "late", "3")
                    .append(TOPIC, 0, T0.plusSeconds(700), "c", "4")
                    .append(TOPIC, 0, T0.plusSeconds(3600), "d", "5");
            startServer();

            // when
            var result = peek("{\"topic\":\"shipments\",\"start_time\":\"2026-11-25T10:05:00Z\",\"until\":\"2026-11-25T10:30:00Z\"}");

            // then
            assertThatJson(result).inPath("records[*].key").isArray()
                    .withFailMessage(() -> "an out-of-order record was returned: " + result).containsExactly("b", "c");
            assertThatJson(result).inPath("outsideTimeRange").isEqualTo(1);
        }

        @Test
        void reportsThatNoRecordIsAtOrAfterStartTime() throws Exception {
            // given
            topicWithPartitions(1);
            appendJson(0, 5);
            startServer();

            // when
            var result = peek("{\"topic\":\"shipments\",\"start_time\":\"2027-01-01T00:00:00Z\"}");

            // then
            assertThatJson(result).inPath("recordsInspected").isEqualTo(0);
            assertThatJson(result).inPath("ranges[0].notes[0]").isString()
                    .withFailMessage(() -> "the missing timestamp was not reported: " + result).contains("no record has a timestamp at or after");
            assertThatJson(result).inPath("exhaustive").isEqualTo(true);
        }

        @Test
        void startsAtTheEarliestOffsetWhenTheRequestedOneWasRemoved() throws Exception {
            // given
            topicWithPartitions(1);
            appendJson(0, 50);
            records.deleteBefore(TOPIC, 0, 30);
            startServer();

            // when
            var result = peek("{\"topic\":\"shipments\",\"start_offset\":10,\"limit\":2}");

            // then
            assertThatJson(result).inPath("ranges[0].requestedStartOffset").isEqualTo(10);
            assertThatJson(result).inPath("ranges[0].rangeStartOffset").isEqualTo(30);
            assertThatJson(result).inPath("ranges[0].notes[0]").isString()
                    .withFailMessage(() -> "the substituted offset was not reported: " + result)
                    .contains("below the earliest available offset 30");
            assertThatJson(result).inPath("records[*].offset").isArray().containsExactly(30, 31);
        }

        @Test
        void reportsAStartOffsetBeyondTheEnd() throws Exception {
            // given
            topicWithPartitions(1);
            appendJson(0, 5);
            startServer();

            // when
            var result = peek("{\"topic\":\"shipments\",\"start_offset\":99}");

            // then
            assertThatJson(result).inPath("recordsInspected").isEqualTo(0);
            assertThatJson(result).inPath("ranges[0].notes[0]").isString()
                    .withFailMessage(() -> "the offset beyond the end was not reported: " + result).contains("beyond the end offset 5");
        }

        @Test
        void readsPastOffsetGapsToFillTheLimit() throws Exception {
            // given
            topicWithPartitions(1);
            for (long offset = 0; offset < 40; offset += 4) {
                records.appendAt(TOPIC, 0, offset, T0.plusSeconds(offset), utf8("k" + offset), utf8("v"));
            }
            startServer();

            // when
            var result = peek("{\"topic\":\"shipments\",\"limit\":5}");

            // then
            assertThatJson(result).inPath("records[*].offset").isArray()
                    .withFailMessage(() -> "gaps were counted as records: " + result).containsExactly(20, 24, 28, 32, 36);
            assertThatJson(result).inPath("ranges[0].offsetsInRange").isEqualTo(37);
            assertThatJson(result).inPath("limitations").isArray().anySatisfy(limitation ->
                    assertThat(limitation.toString()).contains("upper bound"));
        }

        @Test
        void leavesOutRecordsWrittenWhileInspecting() throws Exception {
            // given
            topicWithPartitions(1);
            appendJson(0, 3);
            records.beforeFirstRead = () -> records.append(TOPIC, 0, T0.plusSeconds(999), "new", "{}");
            startServer();

            // when
            var result = peek("{\"topic\":\"shipments\",\"start_offset\":0}");

            // then
            assertThatJson(result).inPath("records[*].offset").isArray()
                    .withFailMessage(() -> "a record written during inspection was included: " + result).containsExactly(0, 1, 2);
            assertThatJson(result).inPath("ranges[0].logEndOffset").isEqualTo(3);
        }

        @Test
        void rejectsConflictingPositions() throws Exception {
            // given
            topicWithPartitions(3);
            startServer();

            // when / then
            assertThat(peekError("{\"topic\":\"shipments\",\"partitions\":[0],\"start_offset\":1,\"start_time\":\"2026-11-25T10:00:00Z\"}"))
                    .contains("start_offset and start_time cannot be combined");
            assertThat(peekError("{\"topic\":\"shipments\",\"start_offset\":1,\"start_offsets\":{\"0\":1}}"))
                    .contains("start_offset and start_offsets cannot be combined");
            assertThat(peekError("{\"topic\":\"shipments\",\"end_offset\":1,\"end_offsets\":{\"0\":1}}"))
                    .contains("end_offset and end_offsets cannot be combined");
            assertThat(peekError("{\"topic\":\"shipments\",\"start_offsets\":{\"0\":1},\"start_time\":\"2026-11-25T10:00:00Z\"}"))
                    .contains("start_offsets and start_time cannot be combined");
            assertThat(peekError("{\"topic\":\"shipments\",\"partitions\":[0],\"end_offset\":5,\"until\":\"2026-11-25T10:00:00Z\"}"))
                    .contains("end_offset and until cannot be combined");
            assertThat(peekError("{\"topic\":\"shipments\",\"start_offset\":1}"))
                    .contains("start_offset applies to a single partition");
            assertThat(peekError("{\"topic\":\"shipments\",\"partitions\":[0],\"start_offsets\":{\"1\":4}}"))
                    .contains("start_offsets names partition(s) [1] that are not in partitions [0]");
            assertThat(peekError("{\"topic\":\"shipments\",\"partitions\":[0],\"start_offset\":-3}"))
                    .contains("start_offset").contains("minimum");
            assertThat(peekError("{\"topic\":\"shipments\",\"partitions\":[0],\"start_offset\":5,\"end_offset\":5}"))
                    .contains("end_offset is exclusive and must be greater than start_offset");
            assertThat(peekError("{\"topic\":\"shipments\",\"start_time\":\"2026-11-25T10:00:00Z\",\"until\":\"2026-11-25T09:00:00Z\"}"))
                    .contains("until must be after start_time");
            assertThat(peekError("{\"topic\":\"shipments\",\"start_time\":\"yesterday\"}"))
                    .contains("start_time must be an ISO-8601 date-time");
            assertThat(peekError("{\"topic\":\"shipments\",\"startOffset\":5}"))
                    .contains("startOffset");
        }
    }

    @Nested
    class Metadata {

        @Test
        void countsTheOffsetsInATimeRangeWithoutDecodingValues() throws Exception {
            // given
            topicWithPartitions(2);
            for (int i = 0; i < 6; i++) {
                records.append(TOPIC, i % 2, T0.plusSeconds(60L * i), utf8("k" + i), new byte[]{0, 0, 0, 0, 9, 1});
            }
            schemaRegistries = registryThatFailsEveryLookup();
            startServer();

            // when
            var result = peek("{\"topic\":\"shipments\",\"start_time\":\"2026-11-25T10:02:00Z\"}");

            // then
            assertThatJson(result).inPath("ranges[*].offsetsInRange").isArray()
                    .withFailMessage(() -> "the offsets per partition were not counted: " + result).containsExactly(2, 2);
            assertThatJson(result).inPath("records[*].valueStatus").isArray()
                    .withFailMessage(() -> "values were decoded for a metadata question: " + result)
                    .containsOnly("not_decoded");
            assertThatJson(result).inPath("records[*].valueSize").isArray().containsOnly(6);
        }

        @Test
        void searchesKeysWithoutDecodingValues() throws Exception {
            // given
            topicWithPartitions(1);
            records.append(TOPIC, 0, T0, "order-XYZ-1", "not json")
                    .append(TOPIC, 0, T0, "order-abc-2", "not json")
                    .append(TOPIC, 0, T0, "ORDER-xyz-3", "not json");
            schemaRegistries = registryThatFailsEveryLookup();
            startServer();

            // when
            var result = peek("{\"topic\":\"shipments\",\"key_contains\":\"xyz\"}");

            // then
            assertThatJson(result).inPath("records[*].key").isArray()
                    .withFailMessage(() -> "the key search did not match ignoring case: " + result)
                    .containsExactly("order-XYZ-1", "ORDER-xyz-3");
            assertThatJson(result).inPath("records[*].valueStatus").isArray().containsOnly("not_decoded");
            assertThatJson(result).inPath("matches").isEqualTo(2);
            assertThatJson(result).inPath("records[*].matched").isArray().containsOnly(true);
        }

        @Test
        void reportsWhichPartitionsHoldRecords() throws Exception {
            // given
            topicWithPartitions(3);
            appendJson(0, 4);
            appendJson(2, 9);
            startServer();

            // when
            var result = peek("{\"topic\":\"shipments\",\"start_time\":\"2026-11-25T10:00:00Z\",\"limit\":1}");

            // then
            assertThatJson(result).inPath("ranges[*].offsetsInRange").isArray()
                    .withFailMessage(() -> "partition activity was not reported: " + result).containsExactly(4, 0, 9);
        }

        @Test
        void reportsKeysThatAreNotTextAsUnsearchable() throws Exception {
            // given
            topicWithPartitions(1);
            records.append(TOPIC, 0, T0, new byte[]{(byte) 0xff, 0x01}, utf8("v"));
            startServer();

            // when
            var result = peek("{\"topic\":\"shipments\",\"key_contains\":\"a\"}");

            // then
            assertThatJson(result).inPath("records[0].keyEncoding").isEqualTo("base64");
            assertThatJson(result).inPath("records[0].key").isEqualTo(Base64.getEncoder().encodeToString(new byte[]{(byte) 0xff, 1}));
            assertThatJson(result).inPath("records[0].matched").withFailMessage(() -> "a binary key was taken for a non-match")
                    .isAbsent();
            assertThatJson(result).inPath("records[0].searchError").isString().contains("binary");
            assertThatJson(result).inPath("unsearchable").isEqualTo(1);
        }
    }

    @Nested
    class Deserialization {

        @Test
        void detectsJson() throws Exception {
            // given
            topicWithPartitions(1);
            records.append(TOPIC, 0, T0, "k", "{\"id\":7,\"tags\":[\"a\"]}");
            startServer();

            // when
            var result = peek("{\"topic\":\"shipments\",\"include_values\":true}");

            // then
            assertThatJson(result).inPath("records[0].format").isEqualTo("json");
            assertThatJson(result).inPath("records[0].value").withFailMessage(() -> "JSON was not decoded: " + result)
                    .isEqualTo("{\"id\":7,\"tags\":[\"a\"]}");
        }

        @Test
        void detectsXml() throws Exception {
            // given
            topicWithPartitions(1);
            records.append(TOPIC, 0, T0, "k", "<order id=\"7\"><line>A</line><line>B</line></order>");
            startServer();

            // when
            var result = peek("{\"topic\":\"shipments\",\"include_values\":true}");

            // then
            assertThatJson(result).inPath("records[0].format").isEqualTo("xml");
            assertThatJson(result).inPath("records[0].value").withFailMessage(() -> "XML was not decoded: " + result)
                    .isEqualTo("{\"@id\":\"7\",\"line\":[\"A\",\"B\"]}");
        }

        @Test
        void detectsSchemaRegistryAvroAndDecodesItWithTheRegisteredSchema() throws Exception {
            // given
            topicWithPartitions(1);
            records.append(TOPIC, 0, T0, utf8("k"), avro(42, "John"));
            schemaRegistries = registryWith(42, CUSTOMER);
            startServer();

            // when
            var result = peek("{\"topic\":\"shipments\",\"include_values\":true}");

            // then
            assertThatJson(result).inPath("records[0].format").isEqualTo("avro");
            assertThatJson(result).inPath("records[0].value").withFailMessage(() -> "Avro was not decoded: " + result)
                    .isEqualTo("{\"name\":\"John\",\"address\":{\"city\":\"Ghent\"}}");
        }

        @Test
        void decodesInTheFormatAskedForInsteadOfDetectingIt() throws Exception {
            // given
            topicWithPartitions(1);
            records.append(TOPIC, 0, T0, "k", "{\"id\":7}");
            startServer();

            // when
            var result = peek("{\"topic\":\"shipments\",\"format\":\"string\",\"include_values\":true}");

            // then
            assertThatJson(result).inPath("records[0].format").isEqualTo("string");
            assertThatJson(result).inPath("records[0].value").withFailMessage(() -> "format string was not honoured")
                    .isString().isEqualTo("{\"id\":7}");
        }

        @Test
        void reportsAValueThatIsNotUtf8AsFailedForFormatString() throws Exception {
            // given
            topicWithPartitions(1);
            records.append(TOPIC, 0, T0, utf8("k"), new byte[]{(byte) 0xc3, 0x28});
            startServer();

            // when
            var result = peek("{\"topic\":\"shipments\",\"format\":\"string\",\"include_values\":true}");

            // then
            assertThatJson(result).inPath("records[0].valueStatus").isEqualTo("failed");
            assertThatJson(result).inPath("records[0].valueError").isString().contains("not valid UTF-8");
        }

        @Test
        void returnsRawValuesAsBase64() throws Exception {
            // given
            topicWithPartitions(1);
            records.append(TOPIC, 0, T0, utf8("k"), new byte[]{1, 2, 3});
            startServer();

            // when
            var result = peek("{\"topic\":\"shipments\",\"format\":\"raw\",\"include_raw\":true}");

            // then
            assertThatJson(result).inPath("records[0].valueStatus").isEqualTo("not_decoded");
            assertThatJson(result).inPath("records[0].raw").isEqualTo("AQID");
        }

        @Test
        void detectsTheFormatOfEachRecord() throws Exception {
            // given
            topicWithPartitions(1);
            records.append(TOPIC, 0, T0, "k", "{\"a\":1}")
                    .append(TOPIC, 0, T0, "k", "<a>1</a>")
                    .append(TOPIC, 0, T0, "k", "{not json")
                    .append(TOPIC, 0, T0, utf8("k"), new byte[]{(byte) 0xff, (byte) 0xfe, 0});
            startServer();

            // when
            var result = peek("{\"topic\":\"shipments\",\"include_values\":true}");

            // then
            assertThatJson(result).inPath("records[*].valueStatus").isArray()
                    .withFailMessage(() -> "one bad record stopped the others decoding: " + result)
                    .containsExactly("decoded", "decoded", "decoded", "unidentified");
            assertThatJson(result).inPath("records[0:3].format").isArray().containsExactly("json", "xml", "string");
        }

        @Test
        void reportsAValueThatDoesNotDecodeAndDecodesTheNext() throws Exception {
            // given
            topicWithPartitions(1);
            records.append(TOPIC, 0, T0, "k", "{\"broken\":")
                    .append(TOPIC, 0, T0, "k", "{\"ok\":true}");
            startServer();

            // when
            var result = peek("{\"topic\":\"shipments\",\"format\":\"json\",\"include_values\":true}");

            // then
            assertThatJson(result).inPath("records[0].valueStatus").isEqualTo("failed");
            assertThatJson(result).inPath("records[0].valueError").isString().contains("not valid JSON");
            assertThatJson(result).inPath("records[1].value").isEqualTo("{\"ok\":true}");
        }

        @Test
        void refusesXmlWithADoctypeInsteadOfResolvingEntities() throws Exception {
            // given
            topicWithPartitions(1);
            records.append(TOPIC, 0, T0, "k", "<?xml version=\"1.0\"?><!DOCTYPE a [<!ENTITY x SYSTEM \"file:///etc/passwd\">]><a>&x;</a>");
            startServer();

            // when
            var result = peek("{\"topic\":\"shipments\",\"format\":\"xml\",\"include_values\":true}");

            // then
            assertThatJson(result).inPath("records[0].valueStatus").withFailMessage(() -> "a DOCTYPE was accepted: " + result)
                    .isEqualTo("failed");
            assertThatJson(result).inPath("records[0].value").isAbsent();
        }

        @Test
        void shortensLargeValuesAndSaysSo() throws Exception {
            // given
            topicWithPartitions(1);
            records.append(TOPIC, 0, T0, "k", "x".repeat(10_000));
            startServer();

            // when
            var result = peek("{\"topic\":\"shipments\",\"include_values\":true}");

            // then
            assertThatJson(result).inPath("records[0].valueTruncated").isEqualTo(true);
            assertThatJson(result).inPath("records[0].value").isString().hasSize(PeekRecordsMcpTool.MAX_VALUE_CHARS);
            assertThatJson(result).inPath("records[0].valueSize").isEqualTo(10_000);
            assertThatJson(result).inPath("truncated").isEqualTo(true);
        }
    }

    @Nested
    class Search {

        @Test
        void returnsTheRecordsWhoseNestedJsonOrXmlValueContainsTheText() throws Exception {
            // given
            topicWithPartitions(1);
            records.append(TOPIC, 0, T0, "a", "{\"customer\":{\"names\":[\"Mary\",\"john smith\"]}}")
                    .append(TOPIC, 0, T0, "b", "{\"customer\":{\"names\":[\"Mary\"]}}")
                    .append(TOPIC, 0, T0, "c", "<order><customer><name>John</name></customer></order>")
                    .append(TOPIC, 0, T0, "d", "{\"John\":\"field names are not searched\"}");
            startServer();

            // when
            var result = peek("{\"topic\":\"shipments\",\"value_contains\":\"JOHN\"}");

            // then
            assertThatJson(result).inPath("records[*].key").isArray()
                    .withFailMessage(() -> "nested values were not searched: " + result).containsExactly("a", "c");
            assertThatJson(result).inPath("matches").isEqualTo(2);
            assertThatJson(result).inPath("exhaustive").isEqualTo(true);
            assertThatJson(result).inPath("summary").isString().contains("2 matched").contains("Every record");
        }

        @Test
        void searchesNestedAvroFields() throws Exception {
            // given
            topicWithPartitions(1);
            records.append(TOPIC, 0, T0, utf8("ann"), avro(42, "Ann"));
            records.append(TOPIC, 0, T0, utf8("bob"), avro(42, "Bob", "Johnstown"));
            schemaRegistries = registryWith(42, CUSTOMER);
            startServer();

            // when
            var result = peek("{\"topic\":\"shipments\",\"value_contains\":\"john\"}");

            // then
            assertThatJson(result).inPath("records[*].key").isArray()
                    .withFailMessage(() -> "the nested Avro field was not searched: " + result).containsExactly("bob");
        }

        @Test
        void reportsNoMatchesOverTheWholeRange() throws Exception {
            // given
            topicWithPartitions(1);
            appendJson(0, 3);
            startServer();

            // when
            var result = peek("{\"topic\":\"shipments\",\"value_contains\":\"John\"}");

            // then
            assertThatJson(result).inPath("matches").isEqualTo(0);
            assertThatJson(result).inPath("records").isArray().isEmpty();
            assertThatJson(result).inPath("exhaustive").isEqualTo(true);
        }

        @Test
        void reportsValuesThatCannotBeDecodedAsUnsearchableNotAsNonMatches() throws Exception {
            // given
            topicWithPartitions(1);
            records.append(TOPIC, 0, T0, utf8("bin"), new byte[]{(byte) 0xff, 0x00, 0x13})
                    .append(TOPIC, 0, T0, "json", "{\"name\":\"John\"}");
            startServer();

            // when
            var result = peek("{\"topic\":\"shipments\",\"value_contains\":\"John\"}");

            // then
            assertThatJson(result).inPath("unsearchable").isEqualTo(1);
            assertThatJson(result).inPath("records[?(@.key=='bin')].matched").withFailMessage(() -> "an undecodable value was "
                    + "taken for a non-match: " + result).isArray().isEmpty();
            assertThatJson(result).inPath("records[?(@.key=='bin')].searchError").isArray().hasSize(1);
            assertThatJson(result).inPath("summary").isString().contains("1 could not be searched");
        }

        @Test
        void reportsABoundedSearchWithoutMatchesAsNotExhaustive() throws Exception {
            // given
            topicWithPartitions(1);
            appendJson(0, 200);
            startServer();

            // when
            var result = peek("{\"topic\":\"shipments\",\"value_contains\":\"John\",\"start_time\":\"2026-11-25T10:00:00Z\"}");

            // then
            assertThatJson(result).inPath("recordsInspected").isEqualTo(50);
            assertThatJson(result).inPath("matches").isEqualTo(0);
            assertThatJson(result).inPath("exhaustive").withFailMessage(() -> "a bounded search claimed to be exhaustive")
                    .isEqualTo(false);
            assertThatJson(result).inPath("limitReached").isEqualTo(true);
            assertThatJson(result).inPath("summary").isString()
                    .contains("stopped at the limit of 50 records")
                    .contains("this does not show that no matching record exists");
        }
    }

    @Nested
    class Mcp {

        @Test
        void advertisesPeekRecordsAsReadOnlyWithItsOutputSchema() throws Exception {
            // given
            startServer();

            // when
            var tools = rpc("tools/list", "{}");

            // then
            assertThat(tools).hasStatusCode(200);
            assertThatJson(tools.body()).inPath("result.tools[?(@.name=='peek_records')].annotations").isArray()
                    .withFailMessage(() -> "peek_records is not advertised read-only: " + tools.body())
                    .containsExactly(Map.of("title", "Peek records", "readOnlyHint", true, "destructiveHint", false,
                            "idempotentHint", true, "openWorldHint", false));
            assertThatJson(tools.body()).inPath("result.tools[?(@.name=='peek_records')].outputSchema.required[*]").isArray()
                    .contains("exhaustive", "limitReached", "recordsInspected", "ranges", "records", "summary");
        }

        @Test
        void answersWithStructuredContentAndTheSameJsonAsText() throws Exception {
            // given
            topicWithPartitions(1);
            appendJson(0, 2);
            startServer();

            // when
            var response = callToolWithArgs("peek_records", "{\"topic\":\"shipments\"}");

            // then
            var structured = structuredContent(response.body());
            assertThatJson(response.body()).inPath("result.content[0].text").isString()
                    .withFailMessage(() -> "the text content is not the structured content")
                    .satisfies(text -> assertThatJson(text).isEqualTo(structured));
        }

        @Test
        void asksTheAgentToGetTheFormatFromTheUserWhenDetectionFails() throws Exception {
            // given
            topicWithPartitions(1);
            records.append(TOPIC, 0, T0, utf8("k"), new byte[]{(byte) 0xff, 0x01});
            startServer();

            // when
            var result = peek("{\"topic\":\"shipments\",\"value_contains\":\"John\"}");

            // then
            assertThatJson(result).inPath("formatUndetermined.records").isEqualTo(1);
            assertThatJson(result).inPath("formatUndetermined.options").isArray().containsExactly("json", "xml", "avro", "string", "raw");
            assertThatJson(result).inPath("formatUndetermined.message").isString().contains("elicitation");
        }

        @Test
        void doesNotAskForAFormatWhenValuesAreNotNeeded() throws Exception {
            // given
            topicWithPartitions(1);
            records.append(TOPIC, 0, T0, utf8("k"), new byte[]{(byte) 0xff, 0x01});
            startServer();

            // when
            var result = peek("{\"topic\":\"shipments\",\"key_contains\":\"k\"}");

            // then
            assertThatJson(result).inPath("formatUndetermined").withFailMessage(() -> "a key question asked for a format")
                    .isAbsent();
        }

        @Test
        void doesNotAskForAFormatTheCallerGave() throws Exception {
            // given
            topicWithPartitions(1);
            records.append(TOPIC, 0, T0, utf8("k"), new byte[]{(byte) 0xff, 0x01});
            startServer();

            // when
            var result = peek("{\"topic\":\"shipments\",\"format\":\"json\",\"include_values\":true}");

            // then
            assertThatJson(result).inPath("formatUndetermined").isAbsent();
            assertThatJson(result).inPath("records[0].valueStatus").isEqualTo("failed");
        }

        @Test
        void refusesAVirtualTopicNamingItsPhysicalTopic() throws Exception {
            // given
            brokerAdmin.states.put("orders-v2", new TopicState(1, 1, Map.of()));
            virtualTopics = new VirtualTopicManager(Map.of("orders", new VirtualTopicConfig("orders-v2",
                    new HeaderEqualsFilterConfig("region", "eu"), true, new JsonFormatConfig())));
            startServer();

            // when
            var error = peekError("{\"topic\":\"orders\"}");

            // then
            assertThat(error).withFailMessage(() -> "the virtual topic was not refused: " + error)
                    .contains("virtual topic").contains("'orders-v2'");
        }

        @Test
        void reportsATopicTheBrokerDoesNotHave() throws Exception {
            // given
            startServer();

            // when
            var error = peekError("{\"topic\":\"nope\"}");

            // then
            assertThat(error).contains("topic 'nope' not found");
        }
    }

    @Nested
    class KafkaSafety {

        @Test
        void closesTheReaderSessionOfEveryCall() throws Exception {
            // given
            topicWithPartitions(2);
            appendJson(0, 3);
            records.failing.add(1);
            startServer();

            // when
            peek("{\"topic\":\"shipments\"}");
            peek("{\"topic\":\"shipments\",\"start_time\":\"2026-11-25T10:00:00Z\"}");

            // then
            assertThat(records.closed).withFailMessage(() -> records.opened + " sessions were opened but " + records.closed + " closed")
                    .isEqualTo(records.opened).isEqualTo(2);
        }

        @Test
        void reportsAPartitionThatCannotBeReadAndInspectsTheOthers() throws Exception {
            // given
            topicWithPartitions(2);
            appendJson(0, 3);
            appendJson(1, 3);
            records.failing.add(1);
            startServer();

            // when
            var result = peek("{\"topic\":\"shipments\"}");

            // then
            assertThatJson(result).inPath("ranges[0].recordsInspected").isEqualTo(3);
            assertThatJson(result).inPath("ranges[1].error").isString().contains("could not read partition 1");
            assertThatJson(result).inPath("exhaustive").isEqualTo(false);
            assertThatJson(result).inPath("summary").isString().contains("partition(s) [1] could not be read");
        }

        @Test
        void reportsATimeoutAsNotExhaustive() throws Exception {
            // given
            topicWithPartitions(2);
            appendJson(0, 3);
            appendJson(1, 3);
            records.timingOut.add(0);
            startServer();

            // when
            var result = peek("{\"topic\":\"shipments\",\"start_offsets\":{\"0\":0,\"1\":0}}");

            // then
            assertThatJson(result).inPath("timedOut").isEqualTo(true);
            assertThatJson(result).inPath("exhaustive").isEqualTo(false);
            assertThatJson(result).inPath("summary").isString().contains("stopped at the time limit");
        }

        @Test
        void reportsABrokerThatDoesNotAnswerAsUninspectedPartitions() throws Exception {
            // given
            topicWithPartitions(2);
            records.rangeError = new IllegalStateException("Timeout of 10000ms expired before offsets could be fetched");
            startServer();

            // when
            var result = peek("{\"topic\":\"shipments\"}");

            // then
            assertThatJson(result).inPath("ranges[*].error").isArray().hasSize(2)
                    .allSatisfy(error -> assertThat(error.toString()).contains("Timeout of 10000ms expired"));
            assertThatJson(result).inPath("exhaustive").isEqualTo(false);
            assertThat(records.closed).isEqualTo(1);
        }
    }

    private static final Schema CUSTOMER = SchemaBuilder.record("Customer").fields()
            .requiredString("name")
            .name("address").type(SchemaBuilder.record("Address").fields().requiredString("city").endRecord()).noDefault()
            .endRecord();

    private void topicWithPartitions(int partitions) {
        brokerAdmin.states.put(TOPIC, new TopicState(partitions, 1, Map.of()));
        records.topic(TOPIC, partitions);
    }

    /// Appends `count` JSON records to `partition`, the n-th at offset n, `T0 + n minutes`, with key `key-n`.
    private void appendJson(int partition, int count) {
        for (int n = 0; n < count; n++) {
            records.append(TOPIC, partition, T0.plusSeconds(60L * n), "key-" + n, "{\"n\":" + n + "}");
        }
    }

    private String peek(String arguments) throws Exception {
        HttpResponse<String> response = callToolWithArgs("peek_records", arguments);
        assertThat(response).hasStatusCode(200);
        assertThatJson(response.body()).inPath("result.structuredContent")
                .withFailMessage(() -> "peek_records answered with an error: " + response.body()).isObject();
        return structuredContent(response.body());
    }

    private String peekError(String arguments) throws Exception {
        HttpResponse<String> response = callToolWithArgs("peek_records", arguments);
        assertThatJson(response.body()).inPath("result.isError")
                .withFailMessage(() -> "peek_records did not refuse " + arguments + ": " + response.body()).isEqualTo(true);
        return new tools.jackson.databind.json.JsonMapper().readTree(response.body())
                .get("result").get("content").get(0).get("text").asString();
    }

    private static byte[] utf8(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static SchemaRegistries registryWith(int schemaId, Schema schema) {
        return SchemaRegistries.of(id -> {
            if (id != schemaId) {
                throw new IllegalStateException("schema id " + id + " not found");
            }
            return schema;
        }, null);
    }

    /// A registry that fails every lookup, so a test fails loudly if a value is decoded.
    private static SchemaRegistries registryThatFailsEveryLookup() {
        return SchemaRegistries.of(id -> {
            throw new AssertionError("schema id " + id + " was looked up");
        }, null);
    }

    private static byte[] avro(int schemaId, String name) throws IOException {
        return avro(schemaId, name, "Ghent");
    }

    /// A `Customer` in the schema registry wire format.
    private static byte[] avro(int schemaId, String name, String city) throws IOException {
        GenericRecord address = new GenericData.Record(CUSTOMER.getField("address").schema());
        address.put("city", city);
        GenericRecord customer = new GenericData.Record(CUSTOMER);
        customer.put("name", name);
        customer.put("address", address);
        var out = new ByteArrayOutputStream();
        out.write(0);
        out.write(ByteBuffer.allocate(4).putInt(schemaId).array());
        var encoder = EncoderFactory.get().binaryEncoder(out, null);
        new GenericDatumWriter<GenericRecord>(CUSTOMER).write(customer, encoder);
        encoder.flush();
        return out.toByteArray();
    }
}

package io.jonasg.kawa.virtualtopic;

import io.jonasg.kawa.config.CelFilterConfig;
import io.jonasg.kawa.config.CelTransformationConfig;
import io.jonasg.kawa.config.ConvertTransformationConfig;
import io.jonasg.kawa.config.DecodeErrorPolicy;
import io.jonasg.kawa.config.DropFieldsTransformationConfig;
import io.jonasg.kawa.config.DropHeadersTransformationConfig;
import io.jonasg.kawa.config.HeaderEqualsFilterConfig;
import io.jonasg.kawa.config.JsonFormatConfig;
import io.jonasg.kawa.config.RenameFieldTransformationConfig;
import io.jonasg.kawa.config.SetHeaderTransformationConfig;
import io.jonasg.kawa.config.TransformationConfig;
import io.jonasg.kawa.config.VirtualTopicConfig;
import io.jonasg.kawa.config.VirtualTopicFilterConfig;
import io.jonasg.kawa.config.XmlFormatConfig;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager.VirtualTopic;
import io.jonasg.kawa.virtualtopic.filter.FailedRecordException;
import io.jonasg.kawa.virtualtopic.filter.SchemaRegistries;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.internal.BaseRecords;
import org.apache.kafka.common.record.internal.ControlRecordType;
import org.apache.kafka.common.record.internal.EndTransactionMarker;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MutableRecordBatch;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/// What a consumer of a virtual topic gets in the `kawa-error` headers of a record the filter or a
/// transformation failed on and that the `include` policy delivers anyway.
class DecodeErrorHeadersTest {

    private static final CelFilterConfig PAID = new CelFilterConfig("value.status == 'PAID'");
    private static final JsonFormatConfig INCLUDE = new JsonFormatConfig(DecodeErrorPolicy.INCLUDE);

    @Test
    void tellsWhyARecordCameThroughUnfiltered() {
        // given a CEL filter on JSON values, and a record that is not JSON
        VirtualTopic topic = topic(PAID, INCLUDE);
        var records = MemoryRecords.withRecords(Compression.NONE, json("{\"status\":\"PAID\"}"), json("card=4111"), json("{\"status\":\"OPEN\"}"));

        // when
        List<Record> delivered = records(fetch(topic, records));

        // then
        assertThat(delivered).extracting(Record::offset)
                .withFailMessage(() -> "The undecodable record was not delivered with the paid one")
                .containsExactly(0L, 1L);
        assertThat(headers(delivered.get(1)))
                .withFailMessage(() -> "The unfiltered record did not say why: " + headers(delivered.get(1)))
                .containsEntry("kawa-error-stage", "filter")
                .doesNotContainKey("kawa-error-step")
                .hasEntrySatisfying("kawa-error", reason -> assertThat(reason)
                        .startsWith("PayloadDecodeException: record value is not valid JSON")
                        .doesNotContain("4111"));
        assertThat(headers(delivered.get(0)))
                .withFailMessage(() -> "A record the filter matched carries error headers: " + headers(delivered.get(0)))
                .isEmpty();
    }

    @Test
    void tellsWhyARecordCameThroughUntransformedNamingTheStepThatFailed() {
        // given a second step that fails on a record whose lines are a list, not an object
        VirtualTopic topic = topic(null, INCLUDE,
                new DropFieldsTransformationConfig(List.of("secret")),
                new RenameFieldTransformationConfig("lines.sku", "sku"),
                new RenameFieldTransformationConfig("status", "state"));
        var records = MemoryRecords.withRecords(Compression.NONE, json("{\"lines\":[{\"sku\":\"A\"}],\"status\":\"PAID\"}"));

        // when
        Record delivered = records(fetch(topic, records)).getFirst();

        // then
        assertThat(value(delivered))
                .withFailMessage(() -> "The record was not delivered as stored: " + value(delivered))
                .isEqualTo("{\"lines\":[{\"sku\":\"A\"}],\"status\":\"PAID\"}");
        assertThat(headers(delivered))
                .withFailMessage(() -> "The untransformed record did not name the failing step: " + headers(delivered))
                .containsEntry("kawa-error-stage", "transformation")
                .containsEntry("kawa-error-step", "1")
                .hasEntrySatisfying("kawa-error", reason -> assertThat(reason).startsWith("PayloadMappingException: transformations[1] (renameField)"));
    }

    @Test
    void blamesTheFirstStepForAValueThatDoesNotDecode() {
        // given
        VirtualTopic topic = topic(null, INCLUDE, new RenameFieldTransformationConfig("a", "b"), new CelTransformationConfig("value"));
        var records = MemoryRecords.withRecords(Compression.NONE, json("not json"));

        // when
        Record delivered = records(fetch(topic, records)).getFirst();

        // then
        assertThat(headers(delivered))
                .withFailMessage(() -> "An undecodable value was not blamed on the first step: " + headers(delivered))
                .containsEntry("kawa-error-stage", "transformation")
                .containsEntry("kawa-error-step", "0")
                .hasEntrySatisfying("kawa-error", reason -> assertThat(reason).startsWith("PayloadDecodeException"));
    }

    @Test
    void blamesTheConvertStepForAResultThatDoesNotEncode() {
        // given a result XML cannot hold: an array as the document
        VirtualTopic topic = topic(null, INCLUDE,
                new CelTransformationConfig("[value.a]"),
                new ConvertTransformationConfig(new XmlFormatConfig(DecodeErrorPolicy.SKIP, "r")));
        var records = MemoryRecords.withRecords(Compression.NONE, json("{\"a\":1}"));

        // when
        Record delivered = records(fetch(topic, records)).getFirst();

        // then
        assertThat(headers(delivered))
                .withFailMessage(() -> "A value that does not encode was not blamed on the convert step: " + headers(delivered))
                .containsEntry("kawa-error-step", "1")
                .hasEntrySatisfying("kawa-error", reason -> assertThat(reason).startsWith("PayloadEncodeException"));
    }

    @Test
    void blamesTheFirstValueStepNotAHeaderStepBeforeIt() {
        // given
        VirtualTopic topic = topic(null, INCLUDE,
                new DropHeadersTransformationConfig(List.of("trace-id")),
                new RenameFieldTransformationConfig("a", "b"));
        var records = MemoryRecords.withRecords(Compression.NONE, json("not json"));

        // when
        Record delivered = records(fetch(topic, records)).getFirst();

        // then
        assertThat(headers(delivered))
                .withFailMessage(() -> "An undecodable value was blamed on the header step: " + headers(delivered))
                .containsEntry("kawa-error-step", "1");
    }

    @Test
    void blamesTheConvertStepNotAHeaderStepAfterIt() {
        // given
        VirtualTopic topic = topic(null, INCLUDE,
                new CelTransformationConfig("[value.a]"),
                new ConvertTransformationConfig(new XmlFormatConfig(DecodeErrorPolicy.SKIP, "r")),
                new SetHeaderTransformationConfig("content-type", "application/xml"));
        var records = MemoryRecords.withRecords(Compression.NONE, json("{\"a\":1}"));

        // when
        Record delivered = records(fetch(topic, records)).getFirst();

        // then
        assertThat(headers(delivered))
                .withFailMessage(() -> "A value that does not encode was not blamed on the convert step: " + headers(delivered))
                .containsEntry("kawa-error-step", "1")
                .doesNotContainEntry("content-type", "application/xml");
    }

    @Test
    void letsNoHeaderStepForgeTheErrorHeaders() {
        // given a header step that sets kawa-error itself, without a convert, so it runs on the stored value
        VirtualTopic topic = topic(null, INCLUDE,
                new RenameFieldTransformationConfig("a", "b"),
                new SetHeaderTransformationConfig("kawa-error", "all good"));
        var records = MemoryRecords.withRecords(Compression.NONE, json("not json"));

        // when
        Record delivered = records(fetch(topic, records)).getFirst();

        // then
        assertThat(Arrays.stream(delivered.headers()).filter(header -> header.key().equals("kawa-error")).count())
                .withFailMessage(() -> "kawa-error was repeated: " + headers(delivered))
                .isEqualTo(1);
        assertThat(headers(delivered).get("kawa-error")).startsWith("PayloadDecodeException");
    }

    @Test
    void tellsWhyARecordCameThroughUnfilteredOnATransformedTopic() {
        // given a filtered and transformed topic, and a record that is not JSON
        VirtualTopic topic = topic(PAID, INCLUDE, new RenameFieldTransformationConfig("status", "state"));
        var records = MemoryRecords.withRecords(Compression.NONE, json("{\"status\":\"PAID\"}"), json("not json"));

        // when
        List<Record> delivered = records(fetch(topic, records));

        // then
        assertThat(delivered).extracting(DecodeErrorHeadersTest::value)
                .containsExactly("{\"state\":\"PAID\"}", "not json");
        assertThat(headers(delivered.get(1)))
                .withFailMessage(() -> "The record was not blamed on the filter: " + headers(delivered.get(1)))
                .containsEntry("kawa-error-stage", "filter")
                .doesNotContainKey("kawa-error-step");
    }

    @Test
    void keepsTheRecordsHeadersAndReplacesStaleErrorHeaders() {
        // given a record already carrying headers, among them error headers from an earlier hop
        VirtualTopic topic = topic(PAID, INCLUDE);
        Header[] stored = {
                new RecordHeader("trace-id", bytes("t-1")),
                new RecordHeader("kawa-error", bytes("stale")),
                new RecordHeader("kawa-error-step", bytes("4")),
                new RecordHeader("tenant", bytes("acme"))};
        var records = MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(1L, bytes("k"), bytes("not json"), stored));

        // when
        Record delivered = records(fetch(topic, records)).getFirst();

        // then
        assertThat(Arrays.stream(delivered.headers()).map(Header::key))
                .withFailMessage(() -> "The record's headers were not kept, or error headers were repeated: "
                        + Arrays.stream(delivered.headers()).map(Header::key).toList())
                .containsExactly("trace-id", "tenant", "kawa-error", "kawa-error-stage");
        assertThat(headers(delivered).get("kawa-error")).isNotEqualTo("stale");
    }

    @Test
    void cutsALongReason() {
        // given a failure message longer than the cap: it names a field with a 300-character name
        String field = String.join("", Collections.nCopies(300, "f"));
        VirtualTopic topic = topic(null, INCLUDE, new RenameFieldTransformationConfig(field + ".sku", "sku"));
        var records = MemoryRecords.withRecords(Compression.NONE, json("{\"" + field + "\":[{\"sku\":\"A\"}]}"));

        // when
        Record delivered = records(fetch(topic, records)).getFirst();

        // then
        assertThat(headers(delivered).get("kawa-error"))
                .withFailMessage(() -> "The reason was not cut at 256 characters: " + headers(delivered).get("kawa-error"))
                .hasSize(256);
    }

    @Test
    void leavesTheHeadersOutWhenTheFilterTurnsThemOff() {
        // given
        VirtualTopic topic = topic(new CelFilterConfig("value.status == 'PAID'", false), INCLUDE);
        var records = MemoryRecords.withRecords(Compression.NONE, json("not json"));

        // when
        List<Record> delivered = records(fetch(topic, records));

        // then
        assertThat(delivered).withFailMessage(() -> "The record was not delivered under include").hasSize(1);
        assertThat(headers(delivered.getFirst()))
                .withFailMessage(() -> "'errorHeader: false' on the filter still added headers: " + headers(delivered.getFirst()))
                .isEmpty();
    }

    @Test
    void leavesTheHeadersOutWhenTheFailingStepTurnsThemOff() {
        // given the failing second step turns them off, the first does not
        VirtualTopic topic = topic(null, INCLUDE,
                new RenameFieldTransformationConfig("status", "state"),
                new RenameFieldTransformationConfig("lines.sku", "sku", false));
        var records = MemoryRecords.withRecords(Compression.NONE, json("{\"lines\":[{\"sku\":\"A\"}],\"status\":\"PAID\"}"));

        // when
        Record delivered = records(fetch(topic, records)).getFirst();

        // then
        assertThat(headers(delivered))
                .withFailMessage(() -> "'errorHeader: false' on the failing step still added headers: " + headers(delivered))
                .isEmpty();
    }

    @ParameterizedTest
    @EnumSource(value = DecodeErrorPolicy.class, names = {"SKIP", "FAIL"})
    void addsNoHeadersUnderAnotherPolicy(DecodeErrorPolicy policy) {
        // given a record that matches and transforms, followed by one that does not decode
        VirtualTopic filtered = topic(PAID, new JsonFormatConfig(policy));
        VirtualTopic transformed = topic(PAID, new JsonFormatConfig(policy), new RenameFieldTransformationConfig("status", "state"));
        var records = MemoryRecords.withRecords(Compression.NONE, json("{\"status\":\"PAID\"}"), json("not json"));

        for (VirtualTopic topic : List.of(filtered, transformed)) {
            // when
            List<Record> delivered = records(fetchUpToAFailure(topic, records));

            // then
            assertThat(delivered).extracting(Record::offset).containsExactly(0L);
            assertThat(delivered).allSatisfy(record -> assertThat(record.headers())
                    .withFailMessage(() -> "A record carries headers under " + policy)
                    .isEmpty());
        }
    }

    @Test
    void keepsTheBatchMetadataTransactionMarkersAndRecordsAroundAFlaggedRecord() {
        // given a transactional batch with an undecodable record, followed by its commit marker
        VirtualTopic topic = topic(PAID, INCLUDE);
        var data = MemoryRecords.withTransactionalRecords(40L, Compression.gzip().build(), 77L, (short) 3, 12, 5,
                new SimpleRecord(1_000L, bytes("k1"), bytes("{\"status\":\"PAID\"}")),
                new SimpleRecord(2_000L, bytes("k2"), bytes("not json")),
                new SimpleRecord(3_000L, bytes("k3"), bytes("{\"status\":\"OPEN\"}")));
        var marker = MemoryRecords.withEndTransactionMarker(43L, 5_000L, 5, 77L, (short) 3,
                new EndTransactionMarker(ControlRecordType.COMMIT, 0));

        // when
        List<MutableRecordBatch> batches = batches(fetch(topic, concat(data, marker)));

        // then
        MutableRecordBatch batch = batches.getFirst();
        batch.ensureValid();
        assertThat(List.of(batch.producerId(), (long) batch.producerEpoch(), (long) batch.baseSequence(), batch.baseOffset(), batch.lastOffset()))
                .withFailMessage(() -> "The batch's producer state or offsets changed")
                .containsExactly(77L, 3L, 12L, 40L, 42L);
        assertThat(batch.isTransactional()).withFailMessage(() -> "The batch is no longer transactional").isTrue();
        List<Record> delivered = new ArrayList<>();
        batch.forEach(delivered::add);
        assertThat(delivered).extracting(Record::offset).containsExactly(40L, 41L);
        assertThat(delivered).extracting(Record::timestamp).containsExactly(1_000L, 2_000L);
        assertThat(delivered).extracting(record -> StandardCharsets.UTF_8.decode(record.key()).toString()).containsExactly("k1", "k2");
        assertThat(batches.get(1).isControlBatch()).withFailMessage(() -> "The commit marker was not delivered").isTrue();
    }

    @Test
    void copiesABatchWithoutFailuresAsItIs() {
        // given
        VirtualTopic topic = topic(PAID, INCLUDE);
        var records = MemoryRecords.withRecords(Compression.NONE, json("{\"status\":\"PAID\"}"));

        // when
        BaseRecords delivered = fetch(topic, records);

        // then
        assertThat(((MemoryRecords) delivered).buffer())
                .withFailMessage(() -> "A batch the filter kept every record of was rebuilt")
                .isEqualTo(records.buffer());
    }

    @Test
    void addsNoHeadersForAHeaderFilter() {
        // given header filters never decode, so they cannot fail on a value
        VirtualTopic topic = topic(new HeaderEqualsFilterConfig("tenant", "acme"), INCLUDE);
        var records = MemoryRecords.withRecords(Compression.NONE,
                new SimpleRecord(1L, bytes("k"), bytes("not json"), new Header[]{new RecordHeader("tenant", bytes("acme"))}));

        // when
        Record delivered = records(fetch(topic, records)).getFirst();

        // then
        assertThat(headers(delivered)).containsOnlyKeys("tenant");
    }

    private static VirtualTopic topic(@Nullable VirtualTopicFilterConfig filter, JsonFormatConfig valueFormat, TransformationConfig... steps) {
        var config = new VirtualTopicConfig("orders.raw", filter, false, valueFormat, List.of(steps), null);
        return new VirtualTopicManager(Map.of("orders", config), SchemaRegistries.NONE).virtualTopic("orders").orElseThrow();
    }

    /// The fetched `records` of `topic` as the fetch path hands them to the consumer.
    private static BaseRecords fetch(VirtualTopic topic, BaseRecords records) {
        return topic.transformer() != null ? topic.transformer().apply(topic.recordFilter(), records) : topic.recordFilter().apply(records);
    }

    /// As [#fetch], or the records before the one the `fail` policy stops at.
    private static BaseRecords fetchUpToAFailure(VirtualTopic topic, BaseRecords records) {
        try {
            return fetch(topic, records);
        } catch (FailedRecordException e) {
            return e.recordsBefore();
        }
    }

    private static Map<String, String> headers(Record record) {
        Map<String, String> headers = new LinkedHashMap<>();
        for (Header header : record.headers()) {
            headers.put(header.key(), new String(header.value(), StandardCharsets.UTF_8));
        }
        return headers;
    }

    private static List<Record> records(BaseRecords records) {
        List<Record> result = new ArrayList<>();
        ((MemoryRecords) records).records().forEach(result::add);
        return result;
    }

    private static List<MutableRecordBatch> batches(BaseRecords records) {
        List<MutableRecordBatch> result = new ArrayList<>();
        ((MemoryRecords) records).batches().forEach(result::add);
        return result;
    }

    private static MemoryRecords concat(MemoryRecords... parts) {
        int size = Arrays.stream(parts).mapToInt(MemoryRecords::sizeInBytes).sum();
        ByteBuffer buffer = ByteBuffer.allocate(size);
        for (MemoryRecords part : parts) {
            buffer.put(part.buffer().duplicate());
        }
        buffer.flip();
        return MemoryRecords.readableRecords(buffer);
    }

    private static String value(Record record) {
        return StandardCharsets.UTF_8.decode(record.value()).toString();
    }

    private static SimpleRecord json(String value) {
        return new SimpleRecord(bytes(value));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}

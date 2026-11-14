package io.jonasg.kawa.virtualtopic.mapping;

import dev.cel.common.values.NullValue;
import io.jonasg.kawa.config.CelFilterConfig;
import io.jonasg.kawa.config.CelTransformationConfig;
import io.jonasg.kawa.config.ConvertTransformationConfig;
import io.jonasg.kawa.config.DefaultFieldTransformationConfig;
import io.jonasg.kawa.config.DropFieldsTransformationConfig;
import io.jonasg.kawa.config.JsonFormatConfig;
import io.jonasg.kawa.config.RenameFieldTransformationConfig;
import io.jonasg.kawa.config.TransformationConfig;
import io.jonasg.kawa.virtualtopic.filter.AvroPayloadDecoder;
import io.jonasg.kawa.virtualtopic.filter.CelRecordPredicate;
import io.jonasg.kawa.virtualtopic.filter.JsonPayloadDecoder;
import io.jonasg.kawa.virtualtopic.filter.JsonPayloadEncoder;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ValueMappingsTest {

    private static final String ORDER = """
            {"id": 7, "status": "PAID", "internal_risk_score": 0.93,
             "customer": {"name": "Ada", "email": "ada@example.com"},
             "city": "Ghent",
             "lines": [{"sku": "A", "qty": 1, "internal_cost": 3.5}, {"sku": "B", "qty": 2, "internal_cost": 1.25}]}
            """;

    private final Record record = record(ORDER);

    @Test
    void dropsTopLevelNestedAndPerListElementFields() {
        // when
        String json = map(ORDER, new DropFieldsTransformationConfig(
                List.of("internal_risk_score", "customer.email", "lines.internal_cost", "not.there")));

        // then
        assertThat(json)
                .withFailMessage(() -> "The fields were not dropped at every level: " + json)
                .isEqualTo("{\"id\":7,\"status\":\"PAID\",\"customer\":{\"name\":\"Ada\"},\"city\":\"Ghent\","
                        + "\"lines\":[{\"sku\":\"A\",\"qty\":1},{\"sku\":\"B\",\"qty\":2}]}");
    }

    @Test
    void renamesInPlaceKeepingTheFieldsPosition() {
        // when
        String json = map("{\"order_id\":7,\"status\":\"PAID\"}", new RenameFieldTransformationConfig("order_id", "id"));

        // then
        assertThat(json)
                .withFailMessage(() -> "The renamed field did not keep its position: " + json)
                .isEqualTo("{\"id\":7,\"status\":\"PAID\"}");
    }

    @Test
    void flattensAndNestsFieldsByRenamingThem() {
        // when
        String json = map("{\"customer\":{\"name\":\"Ada\"},\"city\":\"Ghent\"}",
                new RenameFieldTransformationConfig("customer.name", "customerName"),
                new RenameFieldTransformationConfig("city", "address.city"));

        // then
        assertThat(json)
                .withFailMessage(() -> "The fields were not flattened and nested: " + json)
                .isEqualTo("{\"customer\":{},\"customerName\":\"Ada\",\"address\":{\"city\":\"Ghent\"}}");
    }

    @Test
    void renamesInsideEveryListElement() {
        // when
        String json = map("{\"lines\":[{\"sku\":\"A\"},{\"sku\":\"B\"}]}", new RenameFieldTransformationConfig("lines.sku", "lines.code"));

        // then
        assertThat(json)
                .withFailMessage(() -> "The field was not renamed in every line: " + json)
                .isEqualTo("{\"lines\":[{\"code\":\"A\"},{\"code\":\"B\"}]}");
    }

    @Test
    void ignoresARenameOfAnAbsentField() {
        assertThat(map("{\"a\":1}", new RenameFieldTransformationConfig("b.c", "d"))).isEqualTo("{\"a\":1}");
    }

    @Test
    void refusesARenameOutOfAList() {
        assertThatThrownBy(() -> map(ORDER, new RenameFieldTransformationConfig("lines.sku", "skus")))
                .isInstanceOf(PayloadMappingException.class)
                .hasMessageContaining("transformation #1 (renameField)")
                .hasMessageContaining("'lines' is a list");
    }

    @Test
    void defaultsAbsentAndNullFieldsButKeepsPresentOnes() {
        // when
        String json = map("{\"note\":null,\"currency\":\"USD\"}",
                new DefaultFieldTransformationConfig("note", "n/a"),
                new DefaultFieldTransformationConfig("currency", "EUR"),
                new DefaultFieldTransformationConfig("shipping.method", "post"),
                new DefaultFieldTransformationConfig("tags", List.of("new")),
                new DefaultFieldTransformationConfig("priority", 3));

        // then
        assertThat(json)
                .withFailMessage(() -> "Defaults were not applied to exactly the absent and null fields: " + json)
                .isEqualTo("{\"note\":\"n/a\",\"currency\":\"USD\",\"shipping\":{\"method\":\"post\"},\"tags\":[\"new\"],\"priority\":3}");
    }

    @Test
    void defaultsAFieldInEveryListElement() {
        // when
        String json = map("{\"lines\":[{\"qty\":2},{}]}", new DefaultFieldTransformationConfig("lines.qty", 1));

        // then
        assertThat(json)
                .withFailMessage(() -> "The default was not applied per line: " + json)
                .isEqualTo("{\"lines\":[{\"qty\":2},{\"qty\":1}]}");
    }

    @Test
    void refusesADefaultBelowAScalar() {
        assertThatThrownBy(() -> map("{\"customer\":\"Ada\"}", new DefaultFieldTransformationConfig("customer.name", "x")))
                .isInstanceOf(PayloadMappingException.class)
                .hasMessageContaining("'customer' is not an object");
    }

    @Test
    void replacesTheValueWithACelMapLiteralOfMixedTypes() {
        // when
        String json = map(ORDER, new CelTransformationConfig("""
                {"id": value.id, "status": value.status, "skus": value.lines.map(l, l.sku),
                 "big": value.lines.exists(l, l.qty > 1), "tenant": headers.tenant, "key": key,
                 "note": has(value.note) ? value.note : "none"}
                """));

        // then
        assertThat(json)
                .withFailMessage(() -> "The CEL mapping did not produce the expected value: " + json)
                .isEqualTo("{\"id\":7,\"status\":\"PAID\",\"skus\":[\"A\",\"B\"],\"big\":true,\"tenant\":\"acme\",\"key\":\"order-7\","
                        + "\"note\":\"none\"}");
    }

    @Test
    void refusesACelResultOutsideTheValueTree() {
        assertThatThrownBy(() -> map(ORDER, new CelTransformationConfig("{1: value.id}")))
                .isInstanceOf(PayloadMappingException.class)
                .hasMessageContaining("keys must be strings");
        assertThatThrownBy(() -> map(ORDER, new CelTransformationConfig("{\"n\": 1u}")))
                .isInstanceOf(PayloadMappingException.class)
                .hasMessageContaining("cannot map");
    }

    @Test
    void reportsACelEvaluationErrorForTheRecord() {
        assertThatThrownBy(() -> map(ORDER, new CelTransformationConfig("{\"x\": value.lines[5]}")))
                .isInstanceOf(PayloadMappingException.class)
                .hasMessageContaining("transformation #1 (cel)");
    }

    @Test
    void refusesAnInvalidCelExpressionWhenCompiled() {
        assertThatThrownBy(() -> ValueMappings.compile(List.of(
                new DropFieldsTransformationConfig(List.of("a")),
                new CelTransformationConfig("{\"id\": value.id"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("transformation #2 (cel)");
    }

    @Test
    void appliesStepsInOrderAndLeavesConvertToTheEncoder() {
        // when
        String json = map("{\"order_id\":7}",
                new RenameFieldTransformationConfig("order_id", "id"),
                new CelTransformationConfig("{\"id\": value.id, \"doubled\": value.id * 2}"),
                new ConvertTransformationConfig(new JsonFormatConfig()));

        // then
        assertThat(json)
                .withFailMessage(() -> "The steps were not applied in order: " + json)
                .isEqualTo("{\"id\":7,\"doubled\":14}");
    }

    @Test
    void passesTheValueThroughWithoutSteps() {
        // given
        Object value = decode(ORDER);

        // then
        assertThat(ValueMappings.compile(List.of(new ConvertTransformationConfig(new JsonFormatConfig()))).apply(value, record))
                .withFailMessage(() -> "A mapping without field steps did not return the value itself")
                .isSameAs(value);
    }

    @Test
    void neverModifiesTheDecodedValue() {
        // given
        Object value = decode(ORDER);
        String before = encode(value);

        // when
        map(value, new DropFieldsTransformationConfig(List.of("customer.email", "lines.internal_cost")),
                new RenameFieldTransformationConfig("city", "address.city"),
                new DefaultFieldTransformationConfig("lines.note", "-"));

        // then
        assertThat(encode(value))
                .withFailMessage(() -> "The decoded value was modified by the mapping")
                .isEqualTo(before);
    }

    @Test
    void mapsAReadOnlyAvroView() {
        // given
        Schema schema = new Schema.Parser().parse("""
                {"type": "record", "name": "Order", "fields": [
                  {"name": "id", "type": "long"}, {"name": "secret", "type": "string"}]}
                """);
        var order = new GenericData.Record(schema);
        order.put("id", 7L);
        order.put("secret", "s3cret");
        Object value = new AvroPayloadDecoder(id -> schema).decode(ByteBuffer.wrap(avro(schema, order)));

        // when
        String json = map(value, new DropFieldsTransformationConfig(List.of("secret")), new DefaultFieldTransformationConfig("note", "-"));

        // then
        assertThat(json)
                .withFailMessage(() -> "The Avro view was not mapped: " + json)
                .isEqualTo("{\"id\":7,\"note\":\"-\"}");
    }

    @Test
    void composesWithAFilterOnTheSourceFields() {
        // given a filter on a field the mapping drops
        var filter = new CelRecordPredicate(new CelFilterConfig("value.internal_risk_score < 1.0"), new JsonPayloadDecoder());
        ValueMapping mapping = ValueMappings.compile(List.of(new DropFieldsTransformationConfig(List.of("internal_risk_score"))));

        // when
        boolean kept = filter.test(record);
        String json = encode(mapping.apply(decode(ORDER), record));

        // then
        assertThat(kept)
                .withFailMessage(() -> "The filter on the source field did not keep the record")
                .isTrue();
        assertThat(json)
                .withFailMessage(() -> "The field the filter used was not dropped from the output: " + json)
                .doesNotContain("internal_risk_score");
    }

    @Test
    void readsAMissingFieldOfAMappedValueAsNull() {
        // given
        @SuppressWarnings("unchecked")
        var value = (Map<String, Object>) ValueMappings.compile(List.of(new CelTransformationConfig("{\"id\": value.id}")))
                .apply(decode(ORDER), record);

        // then
        assertThat(value.get("status"))
                .withFailMessage(() -> "A field missing from a CEL result did not read as null")
                .isEqualTo(NullValue.NULL_VALUE);
    }

    private String map(String json, TransformationConfig... steps) {
        return map(decode(json), steps);
    }

    private String map(Object value, TransformationConfig... steps) {
        return encode(ValueMappings.compile(List.of(steps)).apply(value, record));
    }

    private static Object decode(String json) {
        return new JsonPayloadDecoder().decode(ByteBuffer.wrap(json.getBytes(StandardCharsets.UTF_8)));
    }

    private static String encode(Object value) {
        return new String(new JsonPayloadEncoder().encode(value), StandardCharsets.UTF_8);
    }

    private static Record record(String value) {
        var simple = new SimpleRecord(1000L, "order-7".getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8),
                new RecordHeader[] {new RecordHeader("tenant", "acme".getBytes(StandardCharsets.UTF_8))});
        return MemoryRecords.withRecords(Compression.NONE, simple).records().iterator().next();
    }

    private static byte[] avro(Schema schema, Object datum) {
        var out = new ByteArrayOutputStream();
        out.write(0);
        out.writeBytes(ByteBuffer.allocate(4).putInt(1).array());
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        try {
            new GenericDatumWriter<>(schema).write(datum, encoder);
            encoder.flush();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return out.toByteArray();
    }
}

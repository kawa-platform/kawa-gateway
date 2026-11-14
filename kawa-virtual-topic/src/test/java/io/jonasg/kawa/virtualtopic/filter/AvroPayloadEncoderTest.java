package io.jonasg.kawa.virtualtopic.filter;

import dev.cel.common.values.CelByteString;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AvroPayloadEncoderTest {

    static final Schema ORDER = new Schema.Parser().parse("""
            {"type": "record", "name": "Order", "namespace": "shop", "fields": [
              {"name": "id", "type": "long"},
              {"name": "qty", "type": "int"},
              {"name": "status", "type": {"type": "enum", "name": "Status", "symbols": ["NEW", "PAID"]}},
              {"name": "customer", "type": {"type": "record", "name": "Customer", "fields": [{"name": "name", "type": "string"}]}},
              {"name": "lines", "type": {"type": "array", "items": {"type": "record", "name": "Line", "fields": [
                {"name": "sku", "type": "string"}, {"name": "price", "type": "double"}]}}},
              {"name": "tags", "type": {"type": "map", "values": "string"}},
              {"name": "note", "type": ["null", "string"], "default": null},
              {"name": "weight", "type": "float"},
              {"name": "paid", "type": "boolean"},
              {"name": "total", "type": {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}},
              {"name": "signature", "type": "bytes"},
              {"name": "checksum", "type": {"type": "fixed", "name": "Checksum", "size": 4}},
              {"name": "createdAt", "type": {"type": "long", "logicalType": "timestamp-millis"}}
            ]}
            """);

    private static final Schema V1 = record("""
            {"name": "id", "type": "long"}, {"name": "status", "type": "string"}, {"name": "internal_risk_score", "type": "double"}""");
    private static final Schema V2 = record("""
            {"name": "id", "type": "long"}, {"name": "status", "type": "string"}, {"name": "note", "type": "string", "default": "n/a"}""");

    private final AvroPayloadDecoder decoder = new AvroPayloadDecoder(id -> switch (id) {
        case 1 -> ORDER;
        case 10 -> V1;
        case 20 -> V2;
        default -> throw new IllegalStateException("unknown schema id " + id);
    });

    @Test
    void decodingAnEncodedValueGivesTheSameValueBack() {
        // given
        Object value = decoder.decode(ByteBuffer.wrap(AvroRecordPredicateTest.wireFormat(1, ORDER, order())));

        // when
        byte[] encoded = encoder(1, ORDER).encode(value);

        // then
        assertThat(decoder.decode(ByteBuffer.wrap(encoded)))
                .withFailMessage(() -> "The order did not survive an Avro round trip: " + value)
                .isEqualTo(value);
    }

    @Test
    void writesTheTargetSchemaIdInTheWireFormat() {
        // when
        byte[] encoded = encoder(1234, Schema.create(Schema.Type.STRING)).encode("hello");

        // then
        ByteBuffer header = ByteBuffer.wrap(encoded);
        assertThat(header.get())
                .withFailMessage(() -> "The payload does not start with the magic byte 0")
                .isZero();
        assertThat(header.getInt())
                .withFailMessage(() -> "The target subject's schema id was not written after the magic byte")
                .isEqualTo(1234);
    }

    @Test
    void encodesADroppedFieldWithTheTargetSchemasDefault() {
        // given a v1 record whose internal_risk_score a transformation dropped
        Map<String, Object> value = new LinkedHashMap<>(decodeV1());
        value.remove("internal_risk_score");

        // when encoded with v2, which has no risk score but a note with a default
        Object roundTripped = decoder.decode(ByteBuffer.wrap(encoder(20, V2).encode(value)));

        // then
        assertThat(roundTripped)
                .withFailMessage(() -> "The v2 record was not the remaining fields plus the note default: " + roundTripped)
                .isEqualTo(Map.of("id", 42L, "status", "PAID", "note", "n/a"));
    }

    @Test
    void ignoresFieldsTheTargetSchemaDoesNotHave() {
        // when the risk score is not dropped
        Object roundTripped = decoder.decode(ByteBuffer.wrap(encoder(20, V2).encode(decodeV1())));

        // then
        assertThat(roundTripped)
                .withFailMessage(() -> "A field missing from the target schema was not ignored: " + roundTripped)
                .isEqualTo(Map.of("id", 42L, "status", "PAID", "note", "n/a"));
    }

    @Test
    void refusesAMissingFieldWithoutADefault() {
        // given
        Schema withRegion = record("""
                {"name": "id", "type": "long"}, {"name": "region", "type": "string"}""");

        // then
        assertThatThrownBy(() -> encoder(30, withRegion).encode(decodeV1()))
                .isInstanceOf(PayloadEncodeException.class)
                .hasMessageContaining("field 'region' is missing and has no default");
    }

    @Test
    void widensJsonIntegersIntoFloatingPointFields() {
        // given a JSON document whose price is written without a fraction
        Schema line = record("""
                {"name": "sku", "type": "string"}, {"name": "price", "type": "double"}""");
        Object json = new JsonPayloadDecoder().decode(ByteBuffer.wrap("{\"sku\":\"A\",\"price\":1}".getBytes(StandardCharsets.UTF_8)));

        // when
        Object avro = new AvroPayloadDecoder(id -> line).decode(ByteBuffer.wrap(encoder(5, line).encode(json)));

        // then
        assertThat(avro)
                .withFailMessage(() -> "The JSON integer was not written to the double field: " + avro)
                .isEqualTo(Map.of("sku", "A", "price", 1.0));
    }

    @Test
    void prefersTheUnionBranchOfTheValuesOwnType() {
        // given
        Schema union = new Schema.Parser().parse("[\"double\", \"long\"]");

        // when
        Object decoded = new AvroPayloadDecoder(id -> union).decode(ByteBuffer.wrap(encoder(5, union).encode(5L)));

        // then
        assertThat(decoded)
                .withFailMessage(() -> "The long was widened into the double branch: " + decoded)
                .isEqualTo(5L);
    }

    @Test
    void roundsDecimalsHalfEvenToTheSchemasScale() {
        // given
        Schema decimal = new Schema.Parser().parse("{\"type\": \"bytes\", \"logicalType\": \"decimal\", \"precision\": 6, \"scale\": 2}");

        // when
        Object decoded = new AvroPayloadDecoder(id -> decimal).decode(ByteBuffer.wrap(encoder(5, decimal).encode(12.345)));

        // then
        assertThat(decoded)
                .withFailMessage(() -> "12.345 was not rounded half-even to 12.34: " + decoded)
                .isEqualTo(12.34);
        assertThatThrownBy(() -> encoder(5, decimal).encode(123456.0))
                .isInstanceOf(PayloadEncodeException.class)
                .hasMessageContaining("does not fit decimal(6, 2)");
    }

    @Test
    void namesTheFieldPathOfAValueOfTheWrongType() {
        // given
        Map<String, Object> value = new LinkedHashMap<>(decodeOrder());
        value.put("lines", List.of(Map.of("sku", "A", "price", 1.0), Map.of("sku", "B", "price", "free")));

        // then
        assertThatThrownBy(() -> encoder(1, ORDER).encode(value))
                .isInstanceOf(PayloadEncodeException.class)
                .hasMessageContaining("String \"free\" at 'lines[1].price' as Avro double");
    }

    @Test
    void refusesValuesTheSchemaCannotHold() {
        Map<String, Object> order = decodeOrder();
        Map<String, Object> intOverflow = with(order, "qty", (long) Integer.MAX_VALUE + 1);
        Map<String, Object> unknownSymbol = with(order, "status", "SHIPPED");
        Map<String, Object> wrongFixedSize = with(order, "checksum", CelByteString.of(new byte[] {1}));

        for (var invalid : List.of(intOverflow, unknownSymbol, wrongFixedSize)) {
            assertThatThrownBy(() -> encoder(1, ORDER).encode(invalid))
                    .withFailMessage(() -> "An order the schema cannot hold was encoded: " + invalid)
                    .isInstanceOf(PayloadEncodeException.class);
        }
    }

    @Test
    void reportsAnUnresolvableSubject() {
        // given
        var encoder = new AvroPayloadEncoder((subject, version) -> {
            throw new IllegalStateException("HTTP 404");
        }, "orders-value", 3);

        // then
        assertThatThrownBy(() -> encoder.encode("hello"))
                .isInstanceOf(PayloadEncodeException.class)
                .hasMessageContaining("subject 'orders-value' version 3")
                .hasMessageContaining("HTTP 404");
    }

    /// A complete order using every type of [#ORDER].
    static GenericRecord order() {
        var customer = new GenericData.Record(ORDER.getField("customer").schema());
        customer.put("name", "Zoë 東京 🚀");
        var line = new GenericData.Record(ORDER.getField("lines").schema().getElementType());
        line.put("sku", "A");
        line.put("price", 9.99);
        Schema total = ORDER.getField("total").schema();
        var order = new GenericData.Record(ORDER);
        order.put("id", 42L);
        order.put("qty", 3);
        order.put("status", new GenericData.EnumSymbol(ORDER.getField("status").schema(), "PAID"));
        order.put("customer", customer);
        order.put("lines", List.of(line));
        order.put("tags", Map.of("channel", "web"));
        order.put("note", null);
        order.put("weight", 1.5f);
        order.put("paid", true);
        order.put("total", new Conversions.DecimalConversion().toBytes(new BigDecimal("12.34"), total, total.getLogicalType()));
        order.put("signature", ByteBuffer.wrap(new byte[] {1, 2, 3}));
        order.put("checksum", new GenericData.Fixed(ORDER.getField("checksum").schema(), new byte[] {(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF}));
        order.put("createdAt", 1_700_000_000_000L);
        return order;
    }

    private Map<String, Object> decodeOrder() {
        return asMap(decoder.decode(ByteBuffer.wrap(AvroRecordPredicateTest.wireFormat(1, ORDER, order()))));
    }

    private Map<String, Object> decodeV1() {
        var record = new GenericData.Record(V1);
        record.put("id", 42L);
        record.put("status", "PAID");
        record.put("internal_risk_score", 0.93);
        return asMap(decoder.decode(ByteBuffer.wrap(AvroRecordPredicateTest.wireFormat(10, V1, record))));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> asMap(Object value) {
        return (Map<String, Object>) value;
    }

    private static Map<String, Object> with(Map<String, Object> value, String field, Object fieldValue) {
        Map<String, Object> copy = new LinkedHashMap<>(value);
        copy.put(field, fieldValue);
        return copy;
    }

    private static Schema record(String fields) {
        return new Schema.Parser().parse("{\"type\": \"record\", \"name\": \"Rec\", \"fields\": [" + fields + "]}");
    }

    private static AvroPayloadEncoder encoder(int id, Schema schema) {
        return new AvroPayloadEncoder((subject, version) -> new RegisteredAvroSchema(id, schema), "subject-value", null);
    }
}

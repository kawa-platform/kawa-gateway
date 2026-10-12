package io.jonasg.kawa.virtualtopic.filter;

import io.jonasg.kawa.config.AvroFormatConfig;
import io.jonasg.kawa.config.CelFilterConfig;
import io.jonasg.kawa.config.DecodeErrorPolicy;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/// CEL filters over Avro record values decoded by [AvroPayloadDecoder].
class AvroRecordPredicateTest {

    private static final int SCHEMA_ID = 42;

    private static final Schema CUSTOMER = Schema.createRecord("Customer", null, "test", false, List.of(
            new Schema.Field("id", Schema.create(Schema.Type.LONG)),
            new Schema.Field("country", Schema.create(Schema.Type.STRING))));

    private static final Schema ORDER = Schema.createRecord("Order", null, "test", false, List.of(
            new Schema.Field("status", Schema.create(Schema.Type.STRING)),
            new Schema.Field("state", Schema.createEnum("State", null, "test", List.of("OPEN", "SHIPPED"))),
            new Schema.Field("quantity", Schema.create(Schema.Type.INT)),
            new Schema.Field("amount", Schema.create(Schema.Type.DOUBLE)),
            new Schema.Field("price", Schema.create(Schema.Type.FLOAT)),
            new Schema.Field("total", LogicalTypes.decimal(10, 2).addToSchema(Schema.create(Schema.Type.BYTES))),
            new Schema.Field("express", Schema.create(Schema.Type.BOOLEAN)),
            new Schema.Field("customer", CUSTOMER),
            new Schema.Field("tags", Schema.createArray(Schema.create(Schema.Type.STRING))),
            new Schema.Field("attributes", Schema.createMap(Schema.create(Schema.Type.STRING))),
            new Schema.Field("refundId", Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING))),
            new Schema.Field("signature", Schema.create(Schema.Type.BYTES))));

    private static final AvroSchemaResolver SCHEMAS = id -> {
        if (id != SCHEMA_ID) {
            throw new IllegalArgumentException("unknown schema id " + id);
        }
        return ORDER;
    };

    private static final AvroPayloadDecoder DECODER = new AvroPayloadDecoder(SCHEMAS);

    @Test
    void matchesOnStringField() {
        // given
        var predicate = avroFilter("value.status == \"PAID\"");

        // when / then
        assertThat(predicate.test(order(o -> o.put("status", "PAID"))))
                .withFailMessage(() -> "Record with status PAID was not matched")
                .isTrue();
        assertThat(predicate.test(order(o -> o.put("status", "OPEN"))))
                .withFailMessage(() -> "Record with status OPEN was not rejected")
                .isFalse();
    }

    @Test
    void matchesOnEnumAsString() {
        // given
        var predicate = avroFilter("value.state == \"SHIPPED\"");

        // when
        var matches = predicate.test(order(o -> o.put("state", new GenericData.EnumSymbol(ORDER.getField("state").schema(), "SHIPPED"))));

        // then
        assertThat(matches)
                .withFailMessage(() -> "Record with enum state SHIPPED was not matched")
                .isTrue();
    }

    @Test
    void matchesOnNestedRecordField() {
        // given
        var predicate = avroFilter("value.customer.country == \"BE\" && value.customer.id == 7");

        // when
        var matches = predicate.test(order(o -> {
        }));

        // then
        assertThat(matches)
                .withFailMessage(() -> "Record with nested customer BE/7 was not matched")
                .isTrue();
    }

    @Test
    void comparesIntDoubleFloatAndDecimalNumbers() {
        // given
        var predicate = avroFilter("value.quantity > 2 && value.amount > 100 && value.price == 9.99 && value.total == 19.98");

        // when
        var matches = predicate.test(order(o -> {
        }));

        // then
        assertThat(matches)
                .withFailMessage(() -> "Int, double, float and decimal fields were not compared as CEL numbers")
                .isTrue();
    }

    @Test
    void matchesOnArrayAndMapContents() {
        // given
        var predicate = avroFilter("\"urgent\" in value.tags && value.attributes.region == \"eu\" && value.tags.size() == 2");

        // when
        var matches = predicate.test(order(o -> {
        }));

        // then
        assertThat(matches)
                .withFailMessage(() -> "Array element and map entry were not matched")
                .isTrue();
    }

    @Test
    void nullUnionBranchEqualsNullAndStringBranchIsReadable() {
        // given
        var isNull = avroFilter("value.refundId == null");
        var isRefund = avroFilter("value.refundId == \"r-1\"");

        // when / then
        assertThat(isNull.test(order(o -> {
        })))
                .withFailMessage(() -> "Null union branch was not equal to CEL null")
                .isTrue();
        assertThat(isRefund.test(order(o -> o.put("refundId", "r-1"))))
                .withFailMessage(() -> "String union branch was not matched")
                .isTrue();
    }

    @Test
    void bytesFieldIsCelBytes() {
        // given
        var predicate = avroFilter("value.signature == b\"sig\"");

        // when
        var matches = predicate.test(order(o -> {
        }));

        // then
        assertThat(matches)
                .withFailMessage(() -> "Bytes field was not equal to the CEL bytes literal")
                .isTrue();
    }

    @Test
    void missingFieldsDoNotMatchInsteadOfFailing() {
        // given
        var unknownField = avroFilter("value.discount == \"PROMO\"");
        var unknownMapKey = avroFilter("value.attributes.channel == \"web\"");
        var hasUnknown = avroFilter("has(value.discount)");
        var hasKnown = avroFilter("has(value.status) && has(value.attributes.region)");

        // when / then
        Record order = order(o -> {
        });
        assertThat(unknownField.test(order))
                .withFailMessage(() -> "Field absent from the schema was not read as null")
                .isFalse();
        assertThat(unknownMapKey.test(order))
                .withFailMessage(() -> "Absent map key was not read as null")
                .isFalse();
        assertThat(hasUnknown.test(order))
                .withFailMessage(() -> "has() was true for a field absent from the schema")
                .isFalse();
        assertThat(hasKnown.test(order))
                .withFailMessage(() -> "has() was false for a schema field and a present map key")
                .isTrue();
    }

    @Test
    void evaluatingFilterDecodesAvroWithTheClustersSchemas() {
        // given
        var filter = new VirtualTopicRecordFilter.EvaluatingRecordFilter(
                new CelFilterConfig("value.status == \"PAID\""), new AvroFormatConfig(), SCHEMAS);

        // when / then
        assertThat(filter.matches(order(o -> o.put("status", "PAID"))))
                .withFailMessage(() -> "Avro record with status PAID was not retained by the record filter")
                .isTrue();
        assertThat(filter.matches(order(o -> o.put("status", "OPEN"))))
                .withFailMessage(() -> "Avro record with status OPEN was not dropped by the record filter")
                .isFalse();
    }

    @Test
    void avroWithoutSchemaRegistryIsHandledPerDecodeErrorPolicy() {
        // given no schema registry, so no Avro decoder
        var skip = new VirtualTopicRecordFilter.EvaluatingRecordFilter(
                new CelFilterConfig("value.status == \"PAID\""), new AvroFormatConfig(DecodeErrorPolicy.SKIP), null);
        var include = new VirtualTopicRecordFilter.EvaluatingRecordFilter(
                new CelFilterConfig("value.status == \"PAID\""), new AvroFormatConfig(DecodeErrorPolicy.INCLUDE), null);
        Record paid = order(o -> o.put("status", "PAID"));

        // when / then
        assertThat(skip.matches(paid))
                .withFailMessage(() -> "Avro record was not dropped with skip policy when no schema registry is configured")
                .isFalse();
        assertThat(include.matches(paid))
                .withFailMessage(() -> "Avro record was not delivered with include policy when no schema registry is configured")
                .isTrue();
    }

    private static CelRecordPredicate avroFilter(String expression) {
        return new CelRecordPredicate(new CelFilterConfig(expression), DECODER);
    }

    /// An order with default values, adjusted by `customizer`, in the schema registry wire format.
    private static Record order(Consumer<GenericRecord> customizer) {
        var customer = new GenericData.Record(CUSTOMER);
        customer.put("id", 7L);
        customer.put("country", "BE");

        var order = new GenericData.Record(ORDER);
        order.put("status", "OPEN");
        order.put("state", new GenericData.EnumSymbol(ORDER.getField("state").schema(), "OPEN"));
        order.put("quantity", 3);
        order.put("amount", 150.5);
        order.put("price", 9.99f);
        order.put("total", new Conversions.DecimalConversion().toBytes(
                new BigDecimal("19.98"), ORDER.getField("total").schema(), LogicalTypes.decimal(10, 2)));
        order.put("express", true);
        order.put("customer", customer);
        order.put("tags", List.of("urgent", "eu"));
        order.put("attributes", Map.of("region", "eu"));
        order.put("refundId", null);
        order.put("signature", ByteBuffer.wrap("sig".getBytes(StandardCharsets.UTF_8)));
        customizer.accept(order);

        byte[] value = wireFormat(SCHEMA_ID, ORDER, order);
        var simple = new SimpleRecord(1000L, "k".getBytes(StandardCharsets.UTF_8), value);
        return MemoryRecords.withRecords(Compression.NONE, simple).records().iterator().next();
    }

    static byte[] wireFormat(int schemaId, Schema schema, Object datum) {
        var out = new ByteArrayOutputStream();
        out.write(0);
        out.writeBytes(ByteBuffer.allocate(4).putInt(schemaId).array());
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

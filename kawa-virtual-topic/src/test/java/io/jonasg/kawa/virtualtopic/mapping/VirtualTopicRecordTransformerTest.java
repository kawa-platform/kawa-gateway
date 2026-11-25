package io.jonasg.kawa.virtualtopic.mapping;

import io.jonasg.kawa.config.AvroFormatConfig;
import io.jonasg.kawa.config.CelTransformationConfig;
import io.jonasg.kawa.config.ConvertTransformationConfig;
import io.jonasg.kawa.config.DecodeErrorPolicy;
import io.jonasg.kawa.config.DropFieldsTransformationConfig;
import io.jonasg.kawa.config.DropHeadersTransformationConfig;
import io.jonasg.kawa.config.JsonFormatConfig;
import io.jonasg.kawa.config.PayloadFormatConfig;
import io.jonasg.kawa.config.RenameFieldTransformationConfig;
import io.jonasg.kawa.config.SetHeaderTransformationConfig;
import io.jonasg.kawa.config.TransformationConfig;
import io.jonasg.kawa.config.VirtualTopicConfig;
import io.jonasg.kawa.config.XmlFormatConfig;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager.VirtualTopic;
import io.jonasg.kawa.virtualtopic.filter.AvroPayloadDecoder;
import io.jonasg.kawa.virtualtopic.filter.FailedRecordException;
import io.jonasg.kawa.virtualtopic.filter.RegisteredAvroSchema;
import io.jonasg.kawa.virtualtopic.filter.SchemaRegistries;
import io.jonasg.kawa.virtualtopic.filter.SchemaRegistryUnavailableException;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.internal.BaseRecords;
import org.apache.kafka.common.record.internal.ControlRecordType;
import org.apache.kafka.common.record.internal.EndTransactionMarker;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MutableRecordBatch;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/// Batch-level tests for [VirtualTopicRecordTransformer#apply], the transform pass of the fetch path.
class VirtualTopicRecordTransformerTest {

    @Test
    void transformsJsonValuesIntoXmlWithTheMappingApplied() {
        // given JSON orders, exposed as XML without the risk score and with order_id renamed to id
        VirtualTopic topic = topic(new JsonFormatConfig(),
                new DropFieldsTransformationConfig(List.of("internal_risk_score")),
                new RenameFieldTransformationConfig("order_id", "id"),
                new ConvertTransformationConfig(new XmlFormatConfig(DecodeErrorPolicy.SKIP, "order")));
        var records = MemoryRecords.withRecords(Compression.NONE,
                json("{\"order_id\":\"o-1\",\"status\":\"PAID\",\"internal_risk_score\":7}"),
                json("{\"order_id\":\"o-2\",\"status\":\"OPEN\",\"internal_risk_score\":1}"));

        // when
        List<Record> transformed = records(transform(topic, records));

        // then
        assertThat(transformed).extracting(VirtualTopicRecordTransformerTest::value)
                .withFailMessage(() -> "Values were not exposed as mapped XML: " + transformed.stream().map(VirtualTopicRecordTransformerTest::value).toList())
                .containsExactly("<order><id>o-1</id><status>PAID</status></order>", "<order><id>o-2</id><status>OPEN</status></order>");
    }

    @Test
    void keepsOffsetsKeysTimestampsAndHeadersOfEveryRecord() {
        // given records with keys, timestamps and headers, starting at offset 100
        VirtualTopic topic = topic(new JsonFormatConfig(), new RenameFieldTransformationConfig("a", "b"));
        Header[] headers = {new RecordHeader("content-type", "application/json".getBytes(StandardCharsets.UTF_8))};
        var records = MemoryRecords.withRecords(100L, Compression.NONE,
                new SimpleRecord(1_000L, bytes("k1"), bytes("{\"a\":1}"), headers),
                new SimpleRecord(2_000L, bytes("k2"), bytes("{\"a\":2}"), headers));

        // when
        List<Record> transformed = records(transform(topic, records));

        // then
        assertThat(transformed).extracting(Record::offset)
                .withFailMessage(() -> "Records were renumbered instead of keeping their offsets")
                .containsExactly(100L, 101L);
        assertThat(transformed).extracting(record -> StandardCharsets.UTF_8.decode(record.key()).toString())
                .withFailMessage(() -> "Record keys were not kept")
                .containsExactly("k1", "k2");
        assertThat(transformed).extracting(Record::timestamp)
                .withFailMessage(() -> "Record timestamps were not kept")
                .containsExactly(1_000L, 2_000L);
        assertThat(transformed).allSatisfy(record -> assertThat(record.headers())
                .withFailMessage(() -> "Record headers were not kept")
                .containsExactly(headers));
        assertThat(transformed).extracting(VirtualTopicRecordTransformerTest::value)
                .containsExactly("{\"b\":1}", "{\"b\":2}");
    }

    @Test
    void describesTheConvertedFormatInTheHeadersOfTransformedRecords() {
        // given JSON records with format headers, exposed as XML with the header steps rewriting them
        VirtualTopic topic = topic(new JsonFormatConfig(),
                new ConvertTransformationConfig(new XmlFormatConfig(DecodeErrorPolicy.SKIP, "order")),
                new DropHeadersTransformationConfig(List.of("__TypeId__")),
                new SetHeaderTransformationConfig("content-type", "application/xml"));
        var records = MemoryRecords.withRecords(Compression.NONE,
                new SimpleRecord(0L, null, bytes("{\"id\":\"o-1\"}"), formatHeaders()));

        // when
        Record transformed = records(transform(topic, records)).getFirst();

        // then
        assertThat(transformed.headers())
                .withFailMessage(() -> "The headers do not describe the XML value: " + List.of(transformed.headers()))
                .containsExactly(header("region", "eu"), header("content-type", "application/xml"));
    }

    @Test
    void passesTheValueBytesOfAHeaderOnlyTopicThroughUntouched() {
        // given a header-only topic and a value whose bytes a JSON re-encoding would change
        VirtualTopic topic = headerOnlyTopic(new DropHeadersTransformationConfig(List.of("__TypeId__")));
        var records = MemoryRecords.withRecords(Compression.NONE,
                new SimpleRecord(0L, null, bytes("{ \"a\" :  1.0,\"b\":[ 2 ] }"), formatHeaders()));

        // when
        Record delivered = records(transform(topic, records)).getFirst();

        // then
        assertThat(value(delivered))
                .withFailMessage(() -> "The value bytes were rewritten: " + value(delivered))
                .isEqualTo("{ \"a\" :  1.0,\"b\":[ 2 ] }");
        assertThat(delivered.headers())
                .withFailMessage(() -> "The header was not dropped: " + List.of(delivered.headers()))
                .containsExactly(header("content-type", "application/json"), header("region", "eu"));
    }

    @Test
    void deliversTheStoredAvroValueOfAHeaderOnlyTopicWithoutASubjectByteForByte() {
        // given an Avro topic without a subject that only rewrites headers
        SchemaRegistries registries = SchemaRegistries.of(id -> Schema.create(Schema.Type.STRING), null);
        VirtualTopic topic = topic(registries, new AvroFormatConfig(),
                new DropHeadersTransformationConfig(List.of("__TypeId__")),
                new SetHeaderTransformationConfig("content-type", "application/avro"));
        SimpleRecord stored = avro(9, "o-1");
        byte[] storedValue = toArray(stored.value());
        var records = MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(0L, null, storedValue, formatHeaders()));

        // when
        Record delivered = records(transform(topic, records)).getFirst();

        // then
        assertThat(toArray(delivered.value()))
                .withFailMessage(() -> "The stored Avro value was not delivered byte for byte")
                .isEqualTo(storedValue);
        assertThat(delivered.headers())
                .withFailMessage(() -> "The header steps were not applied: " + List.of(delivered.headers()))
                .containsExactly(header("region", "eu"), header("content-type", "application/avro"));
    }

    @Test
    void deliversTheStoredXmlValueOfAHeaderOnlyTopicWithoutARootByteForByte() {
        // given an XML topic without a root that only rewrites headers, and a value whose bytes a re-encoding would change
        VirtualTopic topic = topic(new XmlFormatConfig(),
                new DropHeadersTransformationConfig(List.of("__TypeId__")),
                new SetHeaderTransformationConfig("content-type", "application/xml"));
        String storedValue = "<?xml version=\"1.0\"?>\n<order  id='o-1'>\n  <status>PAID</status>\n</order>";
        var records = MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(0L, null, bytes(storedValue), formatHeaders()));

        // when
        Record delivered = records(transform(topic, records)).getFirst();

        // then
        assertThat(value(delivered))
                .withFailMessage(() -> "The stored XML value was not delivered byte for byte: " + value(delivered))
                .isEqualTo(storedValue);
        assertThat(delivered.headers())
                .withFailMessage(() -> "The header steps were not applied: " + List.of(delivered.headers()))
                .containsExactly(header("region", "eu"), header("content-type", "application/xml"));
    }

    @Test
    void deliversAValueThatIsNotJsonFromAHeaderOnlyTopic() {
        // given a header-only topic and a value that is not JSON, under the default skip policy
        VirtualTopic topic = headerOnlyTopic(new SetHeaderTransformationConfig("content-type", "text/plain"));
        var records = MemoryRecords.withRecords(Compression.NONE,
                new SimpleRecord(0L, null, bytes("not json"), formatHeaders()));

        // when
        List<Record> delivered = records(transform(topic, records));

        // then
        assertThat(delivered).extracting(VirtualTopicRecordTransformerTest::value)
                .withFailMessage(() -> "The value that does not decode was dropped: " + delivered.size())
                .containsExactly("not json");
        assertThat(delivered.getFirst().headers())
                .withFailMessage(() -> "The header was not set: " + List.of(delivered.getFirst().headers()))
                .containsExactly(header("__TypeId__", "com.acme.Order"), header("region", "eu"),
                        header("content-type", "text/plain"));
    }

    @Test
    void mapsTheHeadersOfAStoredValueDeliveredUnderIncludeWhenNoConvertChangedTheFormat() {
        // given a value step under the include policy, without a convert: the stored value stays in the delivered format
        VirtualTopic topic = topic(new JsonFormatConfig(DecodeErrorPolicy.INCLUDE), new RenameFieldTransformationConfig("a", "b"),
                new DropHeadersTransformationConfig(List.of("__TypeId__")));
        var records = MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(0L, null, bytes("not json"), formatHeaders()));

        // when
        Record delivered = records(transform(topic, records)).getFirst();

        // then
        assertThat(delivered.headers())
                .withFailMessage(() -> "The headers of the stored value were not mapped: " + List.of(delivered.headers()))
                .containsExactly(header("content-type", "application/json"), header("region", "eu"));
    }

    @Test
    void keepsTheStoredHeadersOfARecordDeliveredUntransformedUnderIncludePolicy() {
        // given a value that is not JSON, under the include policy, with header steps
        VirtualTopic topic = topic(new JsonFormatConfig(DecodeErrorPolicy.INCLUDE),
                new ConvertTransformationConfig(new XmlFormatConfig(DecodeErrorPolicy.SKIP, "order")),
                new SetHeaderTransformationConfig("content-type", "application/xml"));
        var records = MemoryRecords.withRecords(Compression.NONE,
                new SimpleRecord(0L, null, bytes("not json"), formatHeaders()));

        // when
        Record delivered = records(transform(topic, records)).getFirst();

        // then
        assertThat(delivered.headers())
                .withFailMessage(() -> "The headers of the stored value were rewritten: " + List.of(delivered.headers()))
                .containsExactly(formatHeaders());
    }

    @Test
    void keepsTheStoredHeadersOfATombstone() {
        // given a tombstone with headers, and header steps
        VirtualTopic topic = topic(new JsonFormatConfig(),
                new DropHeadersTransformationConfig(List.of("content-type", "__TypeId__", "region")));
        var records = MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(0L, bytes("k"), null, formatHeaders()));

        // when
        Record tombstone = records(transform(topic, records)).getFirst();

        // then
        assertThat(tombstone.headers())
                .withFailMessage(() -> "The headers of the tombstone were rewritten: " + List.of(tombstone.headers()))
                .containsExactly(formatHeaders());
    }

    @Test
    void keepsTheMetadataOfATransactionalCompressedBatch() {
        // given a gzip-compressed transactional batch
        VirtualTopic topic = topic(new JsonFormatConfig(), new RenameFieldTransformationConfig("a", "b"));
        var records = MemoryRecords.withTransactionalRecords(40L, Compression.gzip().build(), 77L, (short) 3, 12, 5,
                json("{\"a\":1}"), json("{\"a\":2}"));
        RecordBatch original = records.batches().iterator().next();

        // when
        MutableRecordBatch batch = batches(transform(topic, records)).getFirst();

        // then
        assertThat(batch.isTransactional()).withFailMessage(() -> "Batch is no longer transactional").isTrue();
        assertThat(batch.producerId()).withFailMessage(() -> "Producer id was not kept").isEqualTo(77L);
        assertThat(batch.producerEpoch()).withFailMessage(() -> "Producer epoch was not kept").isEqualTo((short) 3);
        assertThat(batch.baseSequence()).withFailMessage(() -> "Base sequence was not kept").isEqualTo(12);
        assertThat(batch.partitionLeaderEpoch()).withFailMessage(() -> "Partition leader epoch was not kept").isEqualTo(5);
        assertThat(batch.compressionType()).withFailMessage(() -> "Compression was not kept").isEqualTo(original.compressionType());
        assertThat(batch.baseOffset()).withFailMessage(() -> "Base offset was not kept").isEqualTo(40L);
        assertThat(batch.lastOffset()).withFailMessage(() -> "Last offset was not kept").isEqualTo(41L);
        batch.ensureValid();
    }

    @Test
    void copiesControlBatchesUnchanged() {
        // given a transactional batch followed by its commit marker
        VirtualTopic topic = topic(new JsonFormatConfig(), new RenameFieldTransformationConfig("a", "b"));
        var data = MemoryRecords.withTransactionalRecords(0L, Compression.NONE, 77L, (short) 3, 0, 5, json("{\"a\":1}"));
        var marker = MemoryRecords.withEndTransactionMarker(1L, 5_000L, 5, 77L, (short) 3,
                new EndTransactionMarker(ControlRecordType.COMMIT, 0));

        // when
        List<MutableRecordBatch> batches = batches(transform(topic, concat(data, marker)));

        // then
        assertThat(batches).hasSize(2);
        MutableRecordBatch control = batches.get(1);
        assertThat(control.isControlBatch()).withFailMessage(() -> "The commit marker is no longer a control batch").isTrue();
        assertThat(control.baseOffset()).withFailMessage(() -> "The commit marker moved").isEqualTo(1L);
        assertThat(ControlRecordType.parse(control.iterator().next().key()))
                .withFailMessage(() -> "The commit marker was changed")
                .isEqualTo(ControlRecordType.COMMIT);
    }

    @Test
    void passesTombstonesThrough() {
        // given a tombstone between two values
        VirtualTopic topic = topic(new JsonFormatConfig(), new RenameFieldTransformationConfig("a", "b"));
        var records = MemoryRecords.withRecords(Compression.NONE,
                json("{\"a\":1}"), new SimpleRecord(bytes("k"), null), json("{\"a\":2}"));

        // when
        List<Record> transformed = records(transform(topic, records));

        // then
        assertThat(transformed).hasSize(3);
        assertThat(transformed.get(1).hasValue()).withFailMessage(() -> "The tombstone got a value").isFalse();
    }

    @Test
    void skipsARecordThatCannotBeTransformedAndKeepsTheOffsetsOfTheOthers() {
        // given a batch with a value that is not JSON, under the default skip policy
        VirtualTopic topic = topic(new JsonFormatConfig(), new RenameFieldTransformationConfig("a", "b"));
        var records = MemoryRecords.withRecords(Compression.NONE, json("{\"a\":1}"), json("not json"), json("{\"a\":3}"));

        // when
        List<Record> transformed = records(transform(topic, records));

        // then
        assertThat(transformed).extracting(Record::offset)
                .withFailMessage(() -> "The undecodable record was not dropped, or the others were renumbered")
                .containsExactly(0L, 2L);
    }

    @Test
    void keepsTheOffsetsOfABatchWhoseRecordsAreAllSkipped() {
        // given a batch in which no value can be transformed
        VirtualTopic topic = topic(new JsonFormatConfig(), new RenameFieldTransformationConfig("a", "b"));
        var records = MemoryRecords.withRecords(10L, Compression.NONE, json("nope"), json("nope either"));

        // when
        BaseRecords result = transform(topic, records);

        // then an empty batch is left, so the consumer still moves past offsets 10 and 11
        assertThat(records(result)).withFailMessage(() -> "Records that cannot be transformed were delivered").isEmpty();
        assertThat(batches(result)).singleElement()
                .extracting(RecordBatch::lastOffset)
                .withFailMessage(() -> "The empty batch does not end at the original last offset")
                .isEqualTo(11L);
    }

    @Test
    void deliversTheStoredValueOfARecordThatCannotBeTransformedUnderIncludePolicy() {
        // given a value that is not JSON, under the include policy
        VirtualTopic topic = topic(new JsonFormatConfig(DecodeErrorPolicy.INCLUDE), new RenameFieldTransformationConfig("a", "b"));
        var records = MemoryRecords.withRecords(Compression.NONE, json("{\"a\":1}"), json("not json"));

        // when
        List<Record> transformed = records(transform(topic, records));

        // then
        assertThat(transformed).extracting(VirtualTopicRecordTransformerTest::value)
                .withFailMessage(() -> "The undecodable value was not delivered as stored")
                .containsExactly("{\"b\":1}", "not json");
    }

    @Test
    void deliversTheTransformedRecordsBeforeARecordThatCannotBeTransformedUnderFailPolicy() {
        // given a batch whose third value is not JSON, under the fail policy
        var records = MemoryRecords.withRecords(Compression.NONE, json("{\"a\":1}"), json("{\"a\":2}"), json("not json"), json("{\"a\":4}"));

        // when
        FailedRecordException failure = failOn(records);

        // then
        assertThat(records(failure.recordsBefore())).extracting(VirtualTopicRecordTransformerTest::value)
                .withFailMessage(() -> "The records before the bad one were not delivered transformed, or a record after it was")
                .containsExactly("{\"b\":1}", "{\"b\":2}");
    }

    @Test
    void stopsThePartitionAtTheOffsetOfARecordThatCannotBeTransformedUnderFailPolicy() {
        // given a batch whose third value is not JSON, under the fail policy
        var records = MemoryRecords.withRecords(Compression.NONE, json("{\"a\":1}"), json("{\"a\":2}"), json("not json"), json("{\"a\":4}"));

        // when
        FailedRecordException failure = failOn(records);

        // then
        assertThat(failure.offset())
                .withFailMessage(() -> "The partition was not stopped at the offset of the bad record, but at " + failure.offset())
                .isEqualTo(2L);
    }

    @Test
    void endsTheDeliveredRecordsRightBeforeARecordThatCannotBeTransformedUnderFailPolicy() {
        // given a batch whose third value is not JSON, under the fail policy
        var records = MemoryRecords.withRecords(Compression.NONE, json("{\"a\":1}"), json("{\"a\":2}"), json("not json"), json("{\"a\":4}"));

        // when
        FailedRecordException failure = failOn(records);

        // then the consumer's next fetch starts at the bad record, not past the original batch
        assertThat(batches(failure.recordsBefore()).getLast().lastOffset())
                .withFailMessage(() -> "The delivered batch does not end right before the bad record, so the consumer would skip it")
                .isEqualTo(1L);
    }

    @Test
    void deliversNothingWhenTheFirstRecordCannotBeTransformedUnderFailPolicy() {
        // given a batch whose first value is not JSON, under the fail policy
        var records = MemoryRecords.withRecords(Compression.NONE, json("not json"), json("{\"a\":2}"));

        // when
        FailedRecordException failure = failOn(records);

        // then
        assertThat(failure.recordsBefore().sizeInBytes())
                .withFailMessage(() -> "Records were delivered although the bad record comes first")
                .isZero();
        assertThat(failure.offset())
                .withFailMessage(() -> "The partition was not stopped at the first record")
                .isZero();
    }

    @Test
    void deliversTheBatchesBeforeTheBatchHoldingARecordThatCannotBeTransformedUnderFailPolicy() {
        // given a batch at offsets 0-1 and a batch at offsets 2-3 that starts with a value that is not JSON
        var records = concat(
                MemoryRecords.withRecords(Compression.NONE, json("{\"a\":1}"), json("{\"a\":2}")),
                MemoryRecords.withRecords(2L, Compression.NONE, json("not json"), json("{\"a\":4}")));

        // when
        FailedRecordException failure = failOn(records);

        // then
        assertThat(records(failure.recordsBefore())).extracting(Record::offset)
                .withFailMessage(() -> "The records of the batch before the bad record were not all delivered")
                .containsExactly(0L, 1L);
        assertThat(failure.offset())
                .withFailMessage(() -> "The partition was not stopped at the bad record of the second batch")
                .isEqualTo(2L);
    }

    private FailedRecordException failOn(MemoryRecords records) {
        VirtualTopic topic = topic(new JsonFormatConfig(DecodeErrorPolicy.FAIL), new RenameFieldTransformationConfig("a", "b"));
        FailedRecordException failure = catchThrowableOfType(FailedRecordException.class, () -> transform(topic, records));
        assertThat(failure)
                .withFailMessage(() -> "A record that cannot be transformed did not stop the partition under the fail policy")
                .isNotNull();
        return failure;
    }

    @Test
    void appliesThePolicyWhenTheMappingFails() {
        // given a cel step that fails on a record without the field it reads
        VirtualTopic topic = topic(new JsonFormatConfig(DecodeErrorPolicy.INCLUDE), new CelTransformationConfig("{\"id\": value.order.id}"));
        var records = MemoryRecords.withRecords(Compression.NONE, json("{\"order\":{\"id\":\"o-1\"}}"), json("{\"order\":5}"));

        // when
        List<Record> transformed = records(transform(topic, records));

        // then
        assertThat(transformed).extracting(VirtualTopicRecordTransformerTest::value)
                .withFailMessage(() -> "The value the mapping failed on was not delivered as stored")
                .containsExactly("{\"id\":\"o-1\"}", "{\"order\":5}");
    }

    @Test
    void appliesThePolicyWhenTheValueCannotBeEncoded() {
        // given an Avro output format on a cluster without a schema registry
        VirtualTopic topic = topic(new JsonFormatConfig(), new ConvertTransformationConfig(new AvroFormatConfig(DecodeErrorPolicy.SKIP, "orders-value", null)));
        var records = MemoryRecords.withRecords(Compression.NONE, json("{\"id\":\"o-1\"}"));

        // when
        List<Record> transformed = records(transform(topic, records));

        // then
        assertThat(transformed).withFailMessage(() -> "The record that cannot be encoded was not skipped").isEmpty();
    }

    @Test
    void encodesTransformedValuesWithTheRegisteredAvroSchema() {
        // given a registry with an Order schema registered under id 7
        Schema order = new Schema.Parser().parse("""
                {"type": "record", "name": "Order", "fields": [
                  {"name": "id", "type": "string"},
                  {"name": "status", "type": "string"}
                ]}""");
        SchemaRegistries registries = SchemaRegistries.of(null, (subject, version) -> new RegisteredAvroSchema(7, order));
        VirtualTopic topic = topic(registries, new JsonFormatConfig(),
                new RenameFieldTransformationConfig("order_id", "id"),
                new ConvertTransformationConfig(new AvroFormatConfig(DecodeErrorPolicy.SKIP, "orders-value", null)));
        var records = MemoryRecords.withRecords(Compression.NONE, json("{\"order_id\":\"o-1\",\"status\":\"PAID\"}"));

        // when
        Record transformed = records(transform(topic, records)).getFirst();

        // then
        Object decoded = new AvroPayloadDecoder(id -> order).decode(transformed.value());
        assertThat(decoded)
                .withFailMessage(() -> "The value was not encoded as the registered Avro schema, decoded: " + decoded)
                .isEqualTo(Map.of("id", "o-1", "status", "PAID"));
        assertThat(transformed.value().getInt(1)).withFailMessage(() -> "The value does not name schema id 7").isEqualTo(7);
    }

    @ParameterizedTest
    @EnumSource(DecodeErrorPolicy.class)
    void failsTheFetchWhenTheRegistryIsUnavailableToDecodeWhateverThePolicy(DecodeErrorPolicy policy) {
        // given a registry that is down while the writer schema is looked up
        var outage = new SchemaRegistryUnavailableException("schema registry is unreachable");
        SchemaRegistries registries = SchemaRegistries.of(id -> {
            throw outage;
        }, null);
        VirtualTopic topic = topic(registries, new AvroFormatConfig(policy), new ConvertTransformationConfig(new JsonFormatConfig()));
        var records = MemoryRecords.withRecords(Compression.NONE, avro(9, "o-1"));

        // when / then the record is neither dropped nor delivered untransformed
        assertThatThrownBy(() -> transform(topic, records))
                .withFailMessage(() -> "A registry outage while decoding was handled by the " + policy + " policy instead of failing the fetch")
                .isSameAs(outage);
    }

    @ParameterizedTest
    @EnumSource(DecodeErrorPolicy.class)
    void failsTheFetchWhenTheRegistryIsUnavailableToEncodeWhateverThePolicy(DecodeErrorPolicy policy) {
        // given a registry that is down while the target schema is looked up
        var outage = new SchemaRegistryUnavailableException("schema registry is unreachable");
        SchemaRegistries registries = SchemaRegistries.of(null, (subject, version) -> {
            throw outage;
        });
        VirtualTopic topic = topic(registries, new JsonFormatConfig(policy),
                new ConvertTransformationConfig(new AvroFormatConfig(policy, "orders-value", null)));
        var records = MemoryRecords.withRecords(Compression.NONE, json("{\"id\":\"o-1\"}"));

        // when / then the record is neither dropped nor delivered untransformed
        assertThatThrownBy(() -> transform(topic, records))
                .withFailMessage(() -> "A registry outage while encoding was handled by the " + policy + " policy instead of failing the fetch")
                .isSameAs(outage);
    }

    @Test
    void skipsARecordWithAnUnknownSchemaId() {
        // given a registry that knows schema id 1 but not id 9
        SchemaRegistries registries = SchemaRegistries.of(id -> {
            if (id == 1) {
                return Schema.create(Schema.Type.STRING);
            }
            throw new IllegalStateException("schema registry returned HTTP 404 for schema id " + id);
        }, null);
        VirtualTopic topic = topic(registries, new AvroFormatConfig(DecodeErrorPolicy.SKIP), new ConvertTransformationConfig(new JsonFormatConfig()));
        var records = MemoryRecords.withRecords(Compression.NONE, avro(9, "o-1"), avro(1, "o-2"));

        // when
        List<Record> transformed = records(transform(topic, records));

        // then
        assertThat(transformed).extracting(Record::offset)
                .withFailMessage(() -> "A record with an unknown schema id was not skipped under the skip policy")
                .containsExactly(1L);
    }

    @Test
    void leavesOutATrailingBatchTheBrokerTruncated() {
        // given a complete batch followed by the first bytes of another
        VirtualTopic topic = topic(new JsonFormatConfig(), new RenameFieldTransformationConfig("a", "b"));
        var complete = MemoryRecords.withRecords(0L, Compression.NONE, json("{\"a\":1}"));
        var next = MemoryRecords.withRecords(1L, Compression.NONE, json("{\"a\":2}"));
        ByteBuffer truncated = next.buffer().duplicate();
        truncated.limit(truncated.limit() - 5);

        // when
        List<Record> transformed = records(transform(topic, concat(complete, MemoryRecords.readableRecords(truncated.slice()))));

        // then
        assertThat(transformed).extracting(Record::offset)
                .withFailMessage(() -> "The truncated batch was not left out")
                .containsExactly(0L);
    }

    @Test
    void upConvertsLegacyBatches() {
        // given a magic v1 batch
        VirtualTopic topic = topic(new JsonFormatConfig(), new RenameFieldTransformationConfig("a", "b"));
        var records = MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V1, 3L, Compression.NONE, TimestampType.CREATE_TIME,
                json("{\"a\":1}"), json("{\"a\":2}"));

        // when
        BaseRecords result = transform(topic, records);

        // then
        assertThat(batches(result)).extracting(RecordBatch::magic).containsOnly(RecordBatch.CURRENT_MAGIC_VALUE);
        assertThat(records(result)).extracting(Record::offset)
                .withFailMessage(() -> "Legacy records were renumbered")
                .containsExactly(3L, 4L);
    }

    @Test
    void leavesTheValuesOfATopicWithoutTransformationsAsStored() {
        // given
        VirtualTopic topic = topic(new JsonFormatConfig());

        // when / then
        assertThat(topic.transformsValues())
                .withFailMessage(() -> "A topic without transformations has its values rewritten")
                .isFalse();
    }

    private static VirtualTopic topic(PayloadFormatConfig valueFormat, TransformationConfig... steps) {
        return topic(SchemaRegistries.NONE, valueFormat, steps);
    }

    private static VirtualTopic topic(SchemaRegistries registries, PayloadFormatConfig valueFormat, TransformationConfig... steps) {
        var config = new VirtualTopicConfig("orders.raw", null, false, valueFormat, List.of(steps), null);
        return new VirtualTopicManager(Map.of("orders", config), registries).virtualTopic("orders").orElseThrow();
    }

    private static VirtualTopic headerOnlyTopic(TransformationConfig... steps) {
        var config = new VirtualTopicConfig("orders.raw", null, false, null, List.of(steps), null);
        return new VirtualTopicManager(Map.of("orders", config), SchemaRegistries.NONE).virtualTopic("orders").orElseThrow();
    }

    /// The fetched `records` of `topic` as its prepared transformer hands them to the consumer.
    private static BaseRecords transform(VirtualTopic topic, BaseRecords records) {
        return Objects.requireNonNull(topic.transformer(), "the topic has no transformations").apply(null, records);
    }

    private static SimpleRecord json(String value) {
        return new SimpleRecord(bytes(value));
    }

    /// A string value in the schema registry wire format.
    private static SimpleRecord avro(int schemaId, String value) {
        var out = new ByteArrayOutputStream();
        out.write(0);
        out.writeBytes(ByteBuffer.allocate(Integer.BYTES).putInt(schemaId).array());
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        try {
            encoder.writeString(value);
            encoder.flush();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return new SimpleRecord(out.toByteArray());
    }

    private static Header[] formatHeaders() {
        return new Header[]{header("content-type", "application/json"), header("__TypeId__", "com.acme.Order"), header("region", "eu")};
    }

    private static Header header(String key, String value) {
        return new RecordHeader(key, bytes(value));
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] toArray(ByteBuffer buffer) {
        ByteBuffer copy = buffer.duplicate();
        byte[] array = new byte[copy.remaining()];
        copy.get(array);
        return array;
    }

    private static String value(Record record) {
        return StandardCharsets.UTF_8.decode(record.value()).toString();
    }

    private static List<Record> records(BaseRecords records) {
        List<Record> all = new ArrayList<>();
        ((MemoryRecords) records).records().forEach(all::add);
        return all;
    }

    private static List<MutableRecordBatch> batches(BaseRecords records) {
        List<MutableRecordBatch> all = new ArrayList<>();
        ((MemoryRecords) records).batches().forEach(all::add);
        return all;
    }

    private static MemoryRecords concat(MemoryRecords... parts) {
        int size = 0;
        for (MemoryRecords part : parts) {
            size += part.sizeInBytes();
        }
        ByteBuffer buffer = ByteBuffer.allocate(size);
        for (MemoryRecords part : parts) {
            buffer.put(part.buffer().duplicate());
        }
        buffer.flip();
        return MemoryRecords.readableRecords(buffer);
    }
}

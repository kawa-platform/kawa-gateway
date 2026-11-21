package io.jonasg.kawa.virtualtopic.transform;

import io.jonasg.kawa.config.AvroFormatConfig;
import io.jonasg.kawa.config.CelFilterConfig;
import io.jonasg.kawa.config.ConvertTransformationConfig;
import io.jonasg.kawa.config.DecodeErrorPolicy;
import io.jonasg.kawa.config.JsonFormatConfig;
import io.jonasg.kawa.config.RenameFieldTransformationConfig;
import io.jonasg.kawa.config.VirtualTopicConfig;
import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.virtualtopic.FetchSessionRegistry;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import io.jonasg.kawa.virtualtopic.filter.SchemaRegistries;
import io.jonasg.kawa.virtualtopic.filter.SchemaRegistryUnavailableException;
import io.jonasg.kawa.virtualtopic.filter.VirtualTopicRecordFilter;
import io.jonasg.kawa.virtualtopic.mapping.VirtualTopicRecordTransformer;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.BaseRecords;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/// Fetch responses of a virtual topic some of whose partitions' records cannot be rewritten - a bad
/// record, a filter error, or a schema registry outage: only those partitions fail, and nothing
/// escapes to the broker connection.
class FetchVirtualTopicTransformTest {

    private static final int KNOWN_SCHEMA_ID = 1;
    private static final int UNAVAILABLE_SCHEMA_ID = 9;

    /// Resolves schema id 1; the registry is down for any other id.
    private final SchemaRegistries registries = SchemaRegistries.of(id -> {
        if (id == KNOWN_SCHEMA_ID) {
            return Schema.create(Schema.Type.STRING);
        }
        throw new SchemaRegistryUnavailableException("schema registry is unreachable");
    }, null);

    private final GatewayContext context = new GatewayContext("test", System.nanoTime());

    @Test
    void failsOnlyThePartitionWhoseRecordsCannotBeTransformedDuringAnOutage() {
        // given a transformed Avro topic under the skip policy
        var transform = transform(new VirtualTopicConfig("orders.raw", null, false, new AvroFormatConfig(DecodeErrorPolicy.SKIP),
                List.of(new ConvertTransformationConfig(new JsonFormatConfig())), null));
        FetchResponseData response = fetch(transform);

        // when
        assertThatCode(() -> transform.onResponse(context, response))
                .withFailMessage(() -> "A registry outage escaped the fetch response, closing the broker connection")
                .doesNotThrowAnyException();

        // then
        assertOutagePartitionFailed(response);
        assertThat(values(partition(response, 1).records()))
                .withFailMessage(() -> "The partition the registry could serve was not transformed as usual")
                .containsExactly("\"o-2\"");
    }

    @Test
    void failsOnlyThePartitionWhoseRecordsCannotBeFilteredDuringAnOutage() {
        // given a filtered Avro topic under the skip policy
        var transform = transform(new VirtualTopicConfig("orders.raw", new CelFilterConfig("value == \"o-2\""), false,
                new AvroFormatConfig(DecodeErrorPolicy.SKIP), List.of(), null));
        FetchResponseData response = fetch(transform);

        // when
        assertThatCode(() -> transform.onResponse(context, response))
                .withFailMessage(() -> "A registry outage escaped the fetch response, closing the broker connection")
                .doesNotThrowAnyException();

        // then
        assertOutagePartitionFailed(response);
        assertThat(((MemoryRecords) partition(response, 1).records()).records())
                .withFailMessage(() -> "The matching record of the partition the registry could serve was filtered out")
                .hasSize(1);
    }

    @Test
    void answersCorruptMessageForAPartitionWhoseFirstRecordCannotBeTransformedUnderFailPolicy() {
        // given a transformed JSON topic under the fail policy, partition 0 starting with a value that is not JSON
        var transform = transform(renamingTopic(DecodeErrorPolicy.FAIL));
        FetchResponseData response = fetch(transform,
                MemoryRecords.withRecords(Compression.NONE, json("not json"), json("{\"a\":2}")),
                MemoryRecords.withRecords(Compression.NONE, json("{\"a\":3}")));

        // when
        assertThatCode(() -> transform.onResponse(context, response))
                .withFailMessage(() -> "A record that cannot be transformed escaped the fetch response, closing the broker connection")
                .doesNotThrowAnyException();

        // then
        assertCorruptMessage(partition(response, 0));
        assertThat(values(partition(response, 1).records()))
                .withFailMessage(() -> "The other partition was not transformed as usual")
                .containsExactly("{\"b\":3}");
    }

    @Test
    void deliversTheRecordsBeforeARecordThatCannotBeTransformedUnderFailPolicy() {
        // given a transformed JSON topic under the fail policy, partition 0 holding a value that is not JSON at offset 1
        var transform = transform(renamingTopic(DecodeErrorPolicy.FAIL));
        FetchResponseData response = fetch(transform,
                MemoryRecords.withRecords(Compression.NONE, json("{\"a\":1}"), json("not json"), json("{\"a\":3}")),
                MemoryRecords.withRecords(Compression.NONE, json("{\"a\":4}")));

        // when
        transform.onResponse(context, response);

        // then
        FetchResponseData.PartitionData stopped = partition(response, 0);
        assertThat(stopped.errorCode())
                .withFailMessage(() -> "The records before the bad one were not delivered without an error, got " + Errors.forCode(stopped.errorCode()))
                .isEqualTo(Errors.NONE.code());
        assertThat(values(stopped.records()))
                .withFailMessage(() -> "Only the record before the bad one should have been delivered, transformed")
                .containsExactly("{\"b\":1}");
        assertThat(values(partition(response, 1).records()))
                .withFailMessage(() -> "The other partition was not transformed as usual")
                .containsExactly("{\"b\":4}");
    }

    @Test
    void answersCorruptMessageForAPartitionWhoseFirstRecordCannotBeFilteredUnderFailPolicy() {
        // given a filtered JSON topic under the fail policy, partition 0 starting with a value that is not JSON
        var transform = transform(new VirtualTopicConfig("orders.raw", new CelFilterConfig("value.a > 0"), false,
                new JsonFormatConfig(DecodeErrorPolicy.FAIL), List.of(), null));
        FetchResponseData response = fetch(transform,
                MemoryRecords.withRecords(Compression.NONE, json("not json")),
                MemoryRecords.withRecords(Compression.NONE, json("{\"a\":3}")));

        // when
        assertThatCode(() -> transform.onResponse(context, response))
                .withFailMessage(() -> "A record that cannot be filtered escaped the fetch response, closing the broker connection")
                .doesNotThrowAnyException();

        // then
        assertCorruptMessage(partition(response, 0));
        assertThat(values(partition(response, 1).records()))
                .withFailMessage(() -> "The matching record of the other partition was not delivered")
                .containsExactly("{\"a\":3}");
    }

    @Test
    void deliversTheFilteredRecordsBeforeARecordThatCannotBeFilteredTransformedUnderFailPolicy() {
        // given a filtered and transformed JSON topic under the fail policy, partition 0 holding a value that is not JSON at offset 2
        var transform = transform(new VirtualTopicConfig("orders.raw", new CelFilterConfig("value.a > 1"), false,
                new JsonFormatConfig(DecodeErrorPolicy.FAIL), List.of(new RenameFieldTransformationConfig("a", "b")), null));
        FetchResponseData response = fetch(transform,
                MemoryRecords.withRecords(Compression.NONE, json("{\"a\":1}"), json("{\"a\":2}"), json("not json"), json("{\"a\":4}")),
                MemoryRecords.withRecords(Compression.NONE, json("{\"a\":5}")));

        // when
        transform.onResponse(context, response);

        // then
        FetchResponseData.PartitionData stopped = partition(response, 0);
        assertThat(stopped.errorCode())
                .withFailMessage(() -> "The records before the bad one were not delivered without an error, got " + Errors.forCode(stopped.errorCode()))
                .isEqualTo(Errors.NONE.code());
        assertThat(values(stopped.records()))
                .withFailMessage(() -> "Only the matching record before the bad one should have been delivered, transformed")
                .containsExactly("{\"b\":2}");
    }

    @Test
    void answersUnknownServerErrorForAPartitionWhoseFilterCannotBeEvaluated() {
        // given a filter that fails to evaluate on partition 0's valid record, under the skip policy
        var transform = transform(new VirtualTopicConfig("orders.raw", new CelFilterConfig("int(value.n) == 1"), false,
                new JsonFormatConfig(DecodeErrorPolicy.SKIP), List.of(), null));
        FetchResponseData response = fetch(transform,
                MemoryRecords.withRecords(Compression.NONE, json("{\"n\":\"not a number\"}")),
                MemoryRecords.withRecords(Compression.NONE, json("{\"n\":\"1\"}")));

        // when
        assertThatCode(() -> transform.onResponse(context, response))
                .withFailMessage(() -> "A filter evaluation error escaped the fetch response, closing the broker connection")
                .doesNotThrowAnyException();

        // then
        FetchResponseData.PartitionData failed = partition(response, 0);
        assertThat(failed.errorCode())
                .withFailMessage(() -> "The partition the filter failed on did not fail with UNKNOWN_SERVER_ERROR, got "
                        + Errors.forCode(failed.errorCode()))
                .isEqualTo(Errors.UNKNOWN_SERVER_ERROR.code());
        assertThat(size(failed.records()))
                .withFailMessage(() -> "The partition the filter failed on still delivered records")
                .isZero();
        assertThat(partition(response, 1).errorCode())
                .withFailMessage(() -> "The other partition was failed as well, got " + Errors.forCode(partition(response, 1).errorCode()))
                .isEqualTo(Errors.NONE.code());
        assertThat(values(partition(response, 1).records()))
                .withFailMessage(() -> "The matching record of the other partition was not delivered")
                .containsExactly("{\"n\":\"1\"}");
    }

    private static void assertCorruptMessage(FetchResponseData.PartitionData partition) {
        assertThat(partition.errorCode())
                .withFailMessage(() -> "The partition starting with a bad record did not answer CORRUPT_MESSAGE, got "
                        + Errors.forCode(partition.errorCode()))
                .isEqualTo(Errors.CORRUPT_MESSAGE.code());
        assertThat(size(partition.records()))
                .withFailMessage(() -> "The partition starting with a bad record still delivered records")
                .isZero();
    }

    private static VirtualTopicConfig renamingTopic(DecodeErrorPolicy policy) {
        return new VirtualTopicConfig("orders.raw", null, false, new JsonFormatConfig(policy),
                List.of(new RenameFieldTransformationConfig("a", "b")), null);
    }

    private static SimpleRecord json(String value) {
        return new SimpleRecord(value.getBytes(StandardCharsets.UTF_8));
    }

    private static void assertOutagePartitionFailed(FetchResponseData response) {
        FetchResponseData.PartitionData failed = partition(response, 0);
        assertThat(failed.errorCode())
                .withFailMessage(() -> "The partition hit by the outage did not fail with UNKNOWN_SERVER_ERROR, got "
                        + Errors.forCode(failed.errorCode()))
                .isEqualTo(Errors.UNKNOWN_SERVER_ERROR.code());
        assertThat(size(failed.records()))
                .withFailMessage(() -> "The partition hit by the outage still delivered records, so the consumer would move past them")
                .isZero();
        assertThat(partition(response, 1).errorCode())
                .withFailMessage(() -> "The partition the registry could serve was failed as well, got "
                        + Errors.forCode(partition(response, 1).errorCode()))
                .isEqualTo(Errors.NONE.code());
    }

    private FetchVirtualTopicTransform transform(VirtualTopicConfig orders) {
        var virtualTopics = new VirtualTopicManager(Map.of("orders", orders));
        return new FetchVirtualTopicTransform(virtualTopics, new FetchSessionRegistry(),
                new VirtualTopicRecordFilter(registries), new VirtualTopicRecordTransformer(registries));
    }

    /// Fetches `orders`; the broker answers with partition 0 holding a record whose schema the
    /// registry cannot serve and partition 1 holding one it can.
    private FetchResponseData fetch(FetchVirtualTopicTransform transform) {
        return fetch(transform,
                MemoryRecords.withRecords(Compression.NONE, avro(UNAVAILABLE_SCHEMA_ID, "o-1")),
                MemoryRecords.withRecords(Compression.NONE, avro(KNOWN_SCHEMA_ID, "o-2")));
    }

    /// Fetches `orders`; the broker answers with `partition0` and `partition1` as partitions 0 and 1.
    private FetchResponseData fetch(FetchVirtualTopicTransform transform, MemoryRecords partition0, MemoryRecords partition1) {
        var request = new FetchRequestData();
        request.topics().add(new FetchRequestData.FetchTopic().setTopic("orders"));
        transform.onRequest(context, request);

        var response = new FetchResponseData();
        response.responses().add(new FetchResponseData.FetchableTopicResponse()
                .setTopic("orders.raw")
                .setPartitions(List.of(
                        new FetchResponseData.PartitionData()
                                .setPartitionIndex(0)
                                .setHighWatermark(1)
                                .setRecords(partition0),
                        new FetchResponseData.PartitionData()
                                .setPartitionIndex(1)
                                .setHighWatermark(1)
                                .setRecords(partition1))));
        return response;
    }

    private static FetchResponseData.PartitionData partition(FetchResponseData response, int index) {
        return response.responses().getFirst().partitions().stream()
                .filter(partition -> partition.partitionIndex() == index)
                .findFirst()
                .orElseThrow();
    }

    private static int size(BaseRecords records) {
        return records == null ? 0 : records.sizeInBytes();
    }

    private static List<String> values(BaseRecords records) {
        List<String> values = new ArrayList<>();
        for (Record record : ((MemoryRecords) records).records()) {
            values.add(StandardCharsets.UTF_8.decode(record.value()).toString());
        }
        return values;
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
        return new SimpleRecord(1000L, "k".getBytes(StandardCharsets.UTF_8), out.toByteArray());
    }
}

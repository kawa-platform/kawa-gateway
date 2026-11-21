package io.jonasg.kawa.virtualtopic.transform;

import io.jonasg.kawa.config.AvroFormatConfig;
import io.jonasg.kawa.config.CelFilterConfig;
import io.jonasg.kawa.config.ConvertTransformationConfig;
import io.jonasg.kawa.config.DecodeErrorPolicy;
import io.jonasg.kawa.config.JsonFormatConfig;
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

/// Fetch responses of a virtual topic whose schema registry is unavailable for some of the
/// partitions' records: only those partitions fail, and nothing escapes to the broker connection.
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
                                .setRecords(MemoryRecords.withRecords(Compression.NONE, avro(UNAVAILABLE_SCHEMA_ID, "o-1"))),
                        new FetchResponseData.PartitionData()
                                .setPartitionIndex(1)
                                .setHighWatermark(1)
                                .setRecords(MemoryRecords.withRecords(Compression.NONE, avro(KNOWN_SCHEMA_ID, "o-2"))))));
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

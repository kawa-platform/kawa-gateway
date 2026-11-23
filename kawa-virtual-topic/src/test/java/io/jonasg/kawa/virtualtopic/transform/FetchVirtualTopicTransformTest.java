package io.jonasg.kawa.virtualtopic.transform;

import io.jonasg.kawa.config.AvroFormatConfig;
import io.jonasg.kawa.config.CelFilterConfig;
import io.jonasg.kawa.config.CelTransformationConfig;
import io.jonasg.kawa.config.ConvertTransformationConfig;
import io.jonasg.kawa.config.DecodeErrorPolicy;
import io.jonasg.kawa.config.HeaderEqualsFilterConfig;
import io.jonasg.kawa.config.JsonFormatConfig;
import io.jonasg.kawa.config.RenameFieldTransformationConfig;
import io.jonasg.kawa.config.VirtualTopicConfig;
import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.virtualtopic.FetchSessionRegistry;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import io.jonasg.kawa.virtualtopic.filter.SchemaRegistries;
import io.jonasg.kawa.virtualtopic.filter.SchemaRegistryUnavailableException;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.protocol.Errors;
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
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/// Fetch responses of a virtual topic some of whose partitions' records cannot be rewritten - a bad
/// record, a filter error, or a schema registry outage: only those partitions fail, and nothing
/// escapes to the broker connection.
class FetchVirtualTopicTransformTest {

    private static final int KNOWN_SCHEMA_ID = 1;
    private static final int UNAVAILABLE_SCHEMA_ID = 9;
    private static final int NO_SESSION = 0;
    private static final int SESSIONLESS = -1;
    private static final int SESSION = 42;

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

    @Test
    void answersUnknownServerErrorForTheTopicWhoseFilterDoesNotCompileWhileOtherTopicsAreServed() {
        // given a configuration in which `orders` has a filter that does not compile, next to the plain topic `payments`
        var virtualTopics = new VirtualTopicManager(Map.of(
                "orders", new VirtualTopicConfig("orders.raw", new CelFilterConfig("value.a >"), false, new JsonFormatConfig()),
                "payments", renamingTopic(DecodeErrorPolicy.SKIP)));
        var transform = new FetchVirtualTopicTransform(virtualTopics, new FetchSessionRegistry());
        FetchResponseData response = fetch(transform,
                MemoryRecords.withRecords(Compression.NONE, json("{\"a\":1}")),
                MemoryRecords.withRecords(Compression.NONE, json("{\"a\":2}")));

        // when
        assertThatCode(() -> transform.onResponse(context, response))
                .withFailMessage(() -> "A filter that does not compile escaped the fetch response, closing the broker connection")
                .doesNotThrowAnyException();

        // then
        assertThat(virtualTopics.virtualTopics())
                .withFailMessage(() -> "A filter that does not compile kept the other virtual topics from being configured")
                .containsKey("payments");
        FetchResponseData.PartitionData failed = partition(response, 0);
        assertThat(failed.errorCode())
                .withFailMessage(() -> "The topic whose filter does not compile did not fail with UNKNOWN_SERVER_ERROR, got "
                        + Errors.forCode(failed.errorCode()))
                .isEqualTo(Errors.UNKNOWN_SERVER_ERROR.code());
        assertThat(size(failed.records()))
                .withFailMessage(() -> "The topic whose filter does not compile still delivered records")
                .isZero();
    }

    @Test
    void deliversTheRecordsBeforeABadRecordInTheMiddleOfABatchEndingRightBeforeIt() {
        // given a transformed topic under the fail policy whose batch at offsets 1-7 holds a bad record at 5
        var transform = transform(renamingTopic(DecodeErrorPolicy.FAIL));

        // when the consumer fetches at offset 1
        FetchResponseData.PartitionData partition = fetchAt(transform, NO_SESSION, SESSIONLESS, 1L, NO_SESSION);

        // then
        assertThat(partition.errorCode())
                .withFailMessage(() -> "The records before the bad one were not delivered without an error, got " + Errors.forCode(partition.errorCode()))
                .isEqualTo(Errors.NONE.code());
        assertThat(offsets(partition.records()))
                .withFailMessage(() -> "Only the records before the bad one should have been delivered")
                .containsExactly(1L, 2L, 3L, 4L);
        assertThat(lastOffset(partition.records()))
                .withFailMessage(() -> "The delivered records do not end right before the bad record, so the consumer would skip it")
                .isEqualTo(4L);
    }

    @Test
    void answersCorruptMessageWhenTheConsumerFetchesAtABadRecordInTheMiddleOfABatch() {
        // given a transformed topic under the fail policy whose batch at offsets 1-7 holds a bad record at 5
        var transform = transform(renamingTopic(DecodeErrorPolicy.FAIL));

        // when the consumer fetches at offset 5, and the broker answers with the whole batch again
        FetchResponseData.PartitionData partition = fetchAt(transform, NO_SESSION, SESSIONLESS, 5L, NO_SESSION);

        // then
        assertCorruptMessage(partition);
    }

    @Test
    void answersCorruptMessageWhenTheConsumerFetchesAtARecordThatCannotBeFilteredInTheMiddleOfABatch() {
        // given a filtered topic under the fail policy whose batch at offsets 1-7 holds a bad record at 5
        var transform = transform(new VirtualTopicConfig("orders.raw", new CelFilterConfig("value.a > 0"), false,
                new JsonFormatConfig(DecodeErrorPolicy.FAIL), List.of(), null));

        // when the consumer fetches at offset 5, and the broker answers with the whole batch again
        FetchResponseData.PartitionData partition = fetchAt(transform, NO_SESSION, SESSIONLESS, 5L, NO_SESSION);

        // then
        assertCorruptMessage(partition);
    }

    @Test
    void answersCorruptMessageAgainWhenAnIncrementalFetchLeavesOutThePartitionStuckAtABadRecord() {
        // given a fetch session the consumer created fetching at the bad record at offset 5
        var transform = transform(renamingTopic(DecodeErrorPolicy.FAIL));
        fetchAt(transform, NO_SESSION, 0, 5L, SESSION);

        // when the consumer retries with an incremental fetch that leaves the unchanged partition out
        FetchResponseData.PartitionData partition = fetchAt(transform, SESSION, 1, null, SESSION);

        // then
        assertCorruptMessage(partition);
    }

    @Test
    void answersCorruptMessageWhenAnIncrementalFetchMovesTheConsumerToABadRecord() {
        // given a fetch session the consumer created fetching at offset 1
        var transform = transform(renamingTopic(DecodeErrorPolicy.FAIL));
        fetchAt(transform, NO_SESSION, 0, 1L, SESSION);

        // when the consumer's incremental fetch moves it to the bad record, then retries leaving the partition out
        fetchAt(transform, SESSION, 1, 5L, SESSION);
        FetchResponseData.PartitionData partition = fetchAt(transform, SESSION, 2, null, SESSION);

        // then
        assertCorruptMessage(partition);
    }

    @Test
    void answersCorruptMessageWhenAnIncrementalFetchAddsAVirtualTopicAtABadRecordToASessionOfPlainTopics() {
        // given a fetch session the consumer created fetching only the plain topic `payments`
        var transform = transform(renamingTopic(DecodeErrorPolicy.FAIL));
        fetchAlongsidePayments(transform, NO_SESSION, 0, 0L, null, false, SESSION);

        // when the consumer's incremental fetch adds `orders` at the bad record at offset 5
        FetchResponseData response = fetchAlongsidePayments(transform, SESSION, 1, null, 5L, true, SESSION);

        // then
        assertCorruptMessage(ordersPartition(response));
    }

    @Test
    void answersCorruptMessageAgainWhenARetryLeavesOutAVirtualTopicAddedToASessionOfPlainTopics() {
        // given a fetch session of the plain topic `payments` to which the consumer added `orders` at the bad record at offset 5
        var transform = transform(renamingTopic(DecodeErrorPolicy.FAIL));
        fetchAlongsidePayments(transform, NO_SESSION, 0, 0L, null, false, SESSION);
        fetchAlongsidePayments(transform, SESSION, 1, null, 5L, true, SESSION);

        // when the consumer retries with an incremental fetch that leaves the unchanged partition out
        FetchResponseData response = fetchAlongsidePayments(transform, SESSION, 2, null, null, true, SESSION);

        // then
        assertCorruptMessage(ordersPartition(response));
    }

    @Test
    void answersAVirtualTopicAddedToASessionOfPlainTopicsUnderItsNameWhenAnIncrementalFetchLeavesItOut() {
        // given a fetch session of the plain topic `payments` to which the consumer added `orders` at offset 1
        var transform = transform(renamingTopic(DecodeErrorPolicy.FAIL));
        fetchAlongsidePayments(transform, NO_SESSION, 0, 0L, null, false, SESSION);
        fetchAlongsidePayments(transform, SESSION, 1, null, 1L, true, SESSION);

        // when the consumer's next incremental fetch leaves `orders` out, and the broker still answers with its records
        FetchResponseData response = fetchAlongsidePayments(transform, SESSION, 2, null, null, true, SESSION);

        // then
        List<String> topics = response.responses().stream().map(FetchResponseData.FetchableTopicResponse::topic).toList();
        assertThat(topics)
                .withFailMessage(() -> "The consumer did not get its records under the virtual topic name `orders` it fetched, got " + topics)
                .containsExactly("orders");
    }

    @Test
    void deliversTheRecordsAfterABadRecordOnceTheConsumerSeeksPastIt() {
        // given a transformed topic under the fail policy whose batch at offsets 1-7 holds a bad record at 5
        var transform = transform(renamingTopic(DecodeErrorPolicy.FAIL));

        // when the consumer seeks to offset 6
        FetchResponseData.PartitionData partition = fetchAt(transform, NO_SESSION, SESSIONLESS, 6L, NO_SESSION);

        // then
        assertThat(partition.errorCode())
                .withFailMessage(() -> "The records after the bad one were not delivered without an error, got " + Errors.forCode(partition.errorCode()))
                .isEqualTo(Errors.NONE.code());
        assertThat(values(partition.records()))
                .withFailMessage(() -> "The records after the bad one were not delivered transformed")
                .containsExactly("{\"b\":6}", "{\"b\":7}");
    }

    @Test
    void deliversOnlyTheRecordsTheFilterKeepsTransformedAtTheirOriginalOffsets() {
        // given a filtered and transformed JSON topic, and a batch at offsets 10-12 of which the filter keeps 11 and 12
        var transform = transform(filteredRenamingTopic(DecodeErrorPolicy.SKIP));
        FetchResponseData response = fetch(transform,
                MemoryRecords.withRecords(10L, Compression.NONE, json("{\"a\":1}"), json("{\"a\":2}"), json("{\"a\":3}")));

        // when
        transform.onResponse(context, response);

        // then
        BaseRecords records = partition(response, 0).records();
        assertThat(values(records))
                .withFailMessage(() -> "Only the records the filter keeps were not delivered, transformed: " + values(records))
                .containsExactly("{\"b\":2}", "{\"b\":3}");
        assertThat(offsets(records))
                .withFailMessage(() -> "The delivered records did not keep their original offsets: " + offsets(records))
                .containsExactly(11L, 12L);
    }

    @Test
    void keepsTheKeysTimestampsAndHeadersOfTheRecordsTheFilterKeeps() {
        // given a header-filtered and transformed JSON topic
        var transform = transform(new VirtualTopicConfig("orders.raw", new HeaderEqualsFilterConfig("tenant", "acme"), false,
                new JsonFormatConfig(), List.of(new RenameFieldTransformationConfig("a", "b")), null));
        Header[] acme = {new RecordHeader("tenant", bytes("acme"))};
        Header[] other = {new RecordHeader("tenant", bytes("other"))};
        FetchResponseData response = fetch(transform, MemoryRecords.withRecords(Compression.NONE,
                new SimpleRecord(1_000L, bytes("k1"), bytes("{\"a\":1}"), other),
                new SimpleRecord(2_000L, bytes("k2"), bytes("{\"a\":2}"), acme)));

        // when
        transform.onResponse(context, response);

        // then
        List<Record> delivered = dataRecords(partition(response, 0).records());
        assertThat(delivered)
                .withFailMessage(() -> "Exactly the record the header filter keeps was not delivered, got " + delivered.size() + " records")
                .hasSize(1);
        Record record = delivered.getFirst();
        assertThat(StandardCharsets.UTF_8.decode(record.key()).toString())
                .withFailMessage(() -> "The key of the delivered record was not kept")
                .isEqualTo("k2");
        assertThat(record.timestamp())
                .withFailMessage(() -> "The timestamp of the delivered record was not kept")
                .isEqualTo(2_000L);
        assertThat(record.headers())
                .withFailMessage(() -> "The headers of the delivered record were not kept")
                .containsExactly(acme);
        assertThat(StandardCharsets.UTF_8.decode(record.value()).toString())
                .withFailMessage(() -> "The delivered record was not transformed")
                .isEqualTo("{\"b\":2}");
    }

    @Test
    void deliversABatchWhoseRecordsAreAllFilteredOutAsAnEmptyBatchKeepingItsOffsetRange() {
        // given a filtered and transformed JSON topic, and a batch at offsets 10-11 the filter drops entirely
        var transform = transform(filteredRenamingTopic(DecodeErrorPolicy.SKIP));
        FetchResponseData response = fetch(transform,
                MemoryRecords.withRecords(10L, Compression.NONE, json("{\"a\":0}"), json("{\"a\":1}")));

        // when
        transform.onResponse(context, response);

        // then the consumer still moves past offsets 10 and 11
        FetchResponseData.PartitionData delivered = partition(response, 0);
        assertThat(delivered.errorCode())
                .withFailMessage(() -> "The fully filtered batch was not delivered without an error, got " + Errors.forCode(delivered.errorCode()))
                .isEqualTo(Errors.NONE.code());
        assertThat(dataRecords(delivered.records()))
                .withFailMessage(() -> "Records the filter drops were delivered")
                .isEmpty();
        List<MutableRecordBatch> batches = batches(delivered.records());
        assertThat(batches)
                .withFailMessage(() -> "The fully filtered batch did not arrive as one empty batch, got " + batches.size() + " batches")
                .hasSize(1);
        assertThat(batches.getFirst().baseOffset())
                .withFailMessage(() -> "The empty batch does not start at the original base offset")
                .isEqualTo(10L);
        assertThat(batches.getFirst().lastOffset())
                .withFailMessage(() -> "The empty batch does not end at the original last offset, so the consumer would fetch it again")
                .isEqualTo(11L);
    }

    @Test
    void keepsTheProducerTransactionAndCompressionOfAFilteredAndTransformedBatch() {
        // given a filtered and transformed JSON topic, and a gzip-compressed transactional batch at offsets 40-42
        var transform = transform(filteredRenamingTopic(DecodeErrorPolicy.SKIP));
        var records = MemoryRecords.withTransactionalRecords(40L, Compression.gzip().build(), 77L, (short) 3, 12, 5,
                json("{\"a\":1}"), json("{\"a\":2}"), json("{\"a\":3}"));
        RecordBatch original = records.batches().iterator().next();
        FetchResponseData response = fetch(transform, records);

        // when
        transform.onResponse(context, response);

        // then
        MutableRecordBatch batch = batches(partition(response, 0).records()).getFirst();
        assertThat(batch.isTransactional()).withFailMessage(() -> "The batch is no longer transactional").isTrue();
        assertThat(batch.producerId()).withFailMessage(() -> "The producer id was not kept").isEqualTo(77L);
        assertThat(batch.producerEpoch()).withFailMessage(() -> "The producer epoch was not kept").isEqualTo((short) 3);
        assertThat(batch.baseSequence()).withFailMessage(() -> "The base sequence was not kept").isEqualTo(12);
        assertThat(batch.compressionType()).withFailMessage(() -> "The compression was not kept").isEqualTo(original.compressionType());
        assertThat(batch.baseOffset()).withFailMessage(() -> "The base offset was not kept").isEqualTo(40L);
        assertThat(batch.lastOffset()).withFailMessage(() -> "The last offset was not kept").isEqualTo(42L);
        batch.ensureValid();
    }

    @Test
    void decodesEachAvroValueOnceWhenItIsBothFilteredAndTransformed() {
        // given an Avro topic with a CEL value filter and a conversion to JSON, over three records of which the filter keeps two
        var decodes = new AtomicInteger();
        var transform = transform(new VirtualTopicConfig("orders.raw", new CelFilterConfig("value != \"o-2\""), false,
                new AvroFormatConfig(DecodeErrorPolicy.SKIP), List.of(new ConvertTransformationConfig(new JsonFormatConfig())), null),
                countingRegistries(decodes));
        FetchResponseData response = fetch(transform, MemoryRecords.withRecords(Compression.NONE,
                avro(KNOWN_SCHEMA_ID, "o-1"), avro(KNOWN_SCHEMA_ID, "o-2"), avro(KNOWN_SCHEMA_ID, "o-3")));

        // when
        transform.onResponse(context, response);

        // then
        assertThat(values(partition(response, 0).records()))
                .withFailMessage(() -> "The records the filter keeps were not delivered as JSON")
                .containsExactly("\"o-1\"", "\"o-3\"");
        assertThat(decodes.get())
                .withFailMessage(() -> "Each of the 3 values was not decoded exactly once, the schema was looked up " + decodes.get() + " times")
                .isEqualTo(3);
    }

    @Test
    void doesNotDecodeTheValuesOfRecordsAHeaderFilterDrops() {
        // given an Avro topic with a header filter and a conversion to JSON, over three records of which the filter keeps one
        var decodes = new AtomicInteger();
        var transform = transform(new VirtualTopicConfig("orders.raw", new HeaderEqualsFilterConfig("tenant", "acme"), false,
                new AvroFormatConfig(DecodeErrorPolicy.SKIP), List.of(new ConvertTransformationConfig(new JsonFormatConfig())), null),
                countingRegistries(decodes));
        FetchResponseData response = fetch(transform, MemoryRecords.withRecords(Compression.NONE,
                avro(KNOWN_SCHEMA_ID, "o-1", "other"), avro(KNOWN_SCHEMA_ID, "o-2", "acme"), avro(KNOWN_SCHEMA_ID, "o-3", "other")));

        // when
        transform.onResponse(context, response);

        // then
        assertThat(values(partition(response, 0).records()))
                .withFailMessage(() -> "The record the header filter keeps was not delivered as JSON")
                .containsExactly("\"o-2\"");
        assertThat(decodes.get())
                .withFailMessage(() -> "Only the value of the record the header filter keeps should have been decoded, the schema was looked up "
                        + decodes.get() + " times")
                .isEqualTo(1);
    }

    @Test
    void skipsAValueThatDoesNotDecodeOnAFilteredAndTransformedTopic() {
        // given a filtered and transformed JSON topic under the skip policy, with a value that is not JSON at offset 1
        var transform = transform(filteredRenamingTopic(DecodeErrorPolicy.SKIP));
        FetchResponseData response = fetch(transform,
                MemoryRecords.withRecords(Compression.NONE, json("{\"a\":2}"), json("not json"), json("{\"a\":3}")));

        // when
        transform.onResponse(context, response);

        // then
        BaseRecords records = partition(response, 0).records();
        assertThat(values(records))
                .withFailMessage(() -> "The value that does not decode was delivered, or the others were not transformed: " + values(records))
                .containsExactly("{\"b\":2}", "{\"b\":3}");
        assertThat(offsets(records))
                .withFailMessage(() -> "The delivered records did not keep their original offsets: " + offsets(records))
                .containsExactly(0L, 2L);
    }

    @Test
    void deliversAValueThatDoesNotDecodeAsStoredOnAFilteredAndTransformedTopicUnderIncludePolicy() {
        // given a filtered and transformed JSON topic under the include policy, with a value that is not JSON at offset 1
        var transform = transform(filteredRenamingTopic(DecodeErrorPolicy.INCLUDE));
        FetchResponseData response = fetch(transform,
                MemoryRecords.withRecords(Compression.NONE, json("{\"a\":2}"), json("not json"), json("{\"a\":0}"), json("{\"a\":3}")));

        // when
        transform.onResponse(context, response);

        // then
        assertThat(values(partition(response, 0).records()))
                .withFailMessage(() -> "The value that does not decode was not delivered as stored between the filtered, transformed records: "
                        + values(partition(response, 0).records()))
                .containsExactly("{\"b\":2}", "not json", "{\"b\":3}");
    }

    @Test
    void answersCorruptMessageForAFilteredAndTransformedPartitionWhoseFirstRecordDoesNotDecodeUnderFailPolicy() {
        // given a filtered and transformed JSON topic under the fail policy, partition 0 starting with a value that is not JSON
        var transform = transform(filteredRenamingTopic(DecodeErrorPolicy.FAIL));
        FetchResponseData response = fetch(transform,
                MemoryRecords.withRecords(Compression.NONE, json("not json"), json("{\"a\":2}")),
                MemoryRecords.withRecords(Compression.NONE, json("{\"a\":3}")));

        // when
        transform.onResponse(context, response);

        // then
        assertCorruptMessage(partition(response, 0));
        assertThat(values(partition(response, 1).records()))
                .withFailMessage(() -> "The other partition was not filtered and transformed as usual")
                .containsExactly("{\"b\":3}");
    }

    @Test
    void skipsARecordTheFilterKeepsButTheTransformationFailsOn() {
        // given a filtered topic whose CEL mapping fails on the kept record at offset 1, under the skip policy
        var transform = transform(filteredMappingTopic(DecodeErrorPolicy.SKIP));
        FetchResponseData response = fetch(transform, MemoryRecords.withRecords(Compression.NONE,
                json("{\"a\":1,\"order\":{\"id\":\"o-1\"}}"), json("{\"a\":1,\"order\":5}"),
                json("{\"a\":0,\"order\":{\"id\":\"o-3\"}}"), json("{\"a\":1,\"order\":{\"id\":\"o-4\"}}")));

        // when
        transform.onResponse(context, response);

        // then
        BaseRecords records = partition(response, 0).records();
        assertThat(values(records))
                .withFailMessage(() -> "The record the mapping fails on was delivered, or the others were not filtered and mapped: " + values(records))
                .containsExactly("{\"id\":\"o-1\"}", "{\"id\":\"o-4\"}");
        assertThat(offsets(records))
                .withFailMessage(() -> "The delivered records did not keep their original offsets: " + offsets(records))
                .containsExactly(0L, 3L);
    }

    @Test
    void deliversARecordTheFilterKeepsButTheTransformationFailsOnAsStoredUnderIncludePolicy() {
        // given a filtered topic whose CEL mapping fails on the kept record at offset 1, under the include policy
        var transform = transform(filteredMappingTopic(DecodeErrorPolicy.INCLUDE));
        FetchResponseData response = fetch(transform, MemoryRecords.withRecords(Compression.NONE,
                json("{\"a\":1,\"order\":{\"id\":\"o-1\"}}"), json("{\"a\":1,\"order\":5}"),
                json("{\"a\":0,\"order\":{\"id\":\"o-3\"}}")));

        // when
        transform.onResponse(context, response);

        // then
        assertThat(values(partition(response, 0).records()))
                .withFailMessage(() -> "The record the mapping fails on was not delivered as stored: " + values(partition(response, 0).records()))
                .containsExactly("{\"id\":\"o-1\"}", "{\"a\":1,\"order\":5}");
    }

    @Test
    void deliversTheRecordsBeforeAKeptRecordTheTransformationFailsOnUnderFailPolicy() {
        // given a filtered topic whose CEL mapping fails on the kept record at offset 2, under the fail policy
        var transform = transform(filteredMappingTopic(DecodeErrorPolicy.FAIL));
        FetchResponseData response = fetch(transform, MemoryRecords.withRecords(Compression.NONE,
                json("{\"a\":1,\"order\":{\"id\":\"o-1\"}}"), json("{\"a\":0,\"order\":{\"id\":\"o-2\"}}"),
                json("{\"a\":1,\"order\":5}"), json("{\"a\":1,\"order\":{\"id\":\"o-4\"}}")));

        // when
        transform.onResponse(context, response);

        // then
        FetchResponseData.PartitionData stopped = partition(response, 0);
        assertThat(stopped.errorCode())
                .withFailMessage(() -> "The records before the bad one were not delivered without an error, got " + Errors.forCode(stopped.errorCode()))
                .isEqualTo(Errors.NONE.code());
        assertThat(values(stopped.records()))
                .withFailMessage(() -> "Only the kept record before the bad one should have been delivered, mapped: " + values(stopped.records()))
                .containsExactly("{\"id\":\"o-1\"}");
        assertThat(lastOffset(stopped.records()))
                .withFailMessage(() -> "The delivered records do not end right before the bad record, so the consumer would skip it")
                .isEqualTo(1L);
    }

    @Test
    void skipsAKeptRecordWhoseTransformedValueCannotBeEncoded() {
        // given a filtered topic converting to Avro on a cluster without a schema registry, under the skip policy
        var transform = transform(new VirtualTopicConfig("orders.raw", new CelFilterConfig("value.a > 1"), false, new JsonFormatConfig(),
                List.of(new ConvertTransformationConfig(new AvroFormatConfig(DecodeErrorPolicy.SKIP, "orders-value", null))), null),
                SchemaRegistries.NONE);
        FetchResponseData response = fetch(transform, MemoryRecords.withRecords(Compression.NONE, json("{\"a\":1}"), json("{\"a\":2}")));

        // when
        transform.onResponse(context, response);

        // then
        FetchResponseData.PartitionData delivered = partition(response, 0);
        assertThat(delivered.errorCode())
                .withFailMessage(() -> "The partition was failed instead of skipping the record, got " + Errors.forCode(delivered.errorCode()))
                .isEqualTo(Errors.NONE.code());
        assertThat(dataRecords(delivered.records()))
                .withFailMessage(() -> "The kept record whose value cannot be encoded was not skipped")
                .isEmpty();
    }

    @Test
    void stopsAtAnEarlierRecordThatCannotBeTransformedRatherThanAtALaterRecordThatCannotBeFiltered() {
        // given a filtered topic under the fail policy whose mapping fails at offset 1 and whose value at offset 3 is not JSON
        var transform = transform(filteredMappingTopic(DecodeErrorPolicy.FAIL));
        FetchResponseData response = fetch(transform, MemoryRecords.withRecords(Compression.NONE,
                json("{\"a\":1,\"order\":{\"id\":\"o-1\"}}"), json("{\"a\":1,\"order\":5}"),
                json("{\"a\":1,\"order\":{\"id\":\"o-3\"}}"), json("not json")));

        // when
        transform.onResponse(context, response);

        // then
        assertStoppedRightAfterTheFirstRecord(partition(response, 0), "{\"id\":\"o-1\"}");
    }

    @Test
    void answersCorruptMessageForAFirstRecordThatCannotBeTransformedRatherThanForALaterRecordThatCannotBeFiltered() {
        // given a filtered topic under the fail policy whose mapping fails at offset 0 and whose value at offset 1 is not JSON
        var transform = transform(filteredMappingTopic(DecodeErrorPolicy.FAIL));
        FetchResponseData response = fetch(transform, MemoryRecords.withRecords(Compression.NONE,
                json("{\"a\":1,\"order\":5}"), json("not json")));

        // when
        transform.onResponse(context, response);

        // then
        assertCorruptMessage(partition(response, 0));
    }

    @Test
    void stopsAtAnEarlierRecordThatCannotBeTransformedRatherThanFailingOnALaterFilterOutage() {
        // given an Avro topic under the fail policy whose mapping fails at offset 1 and whose record at offset 3 has a schema the registry cannot serve
        var transform = transform(new VirtualTopicConfig("orders.raw", new CelFilterConfig("value != \"x\""), false,
                new AvroFormatConfig(DecodeErrorPolicy.FAIL),
                List.of(new CelTransformationConfig("{\"n\": int(value)}"), new ConvertTransformationConfig(new JsonFormatConfig())), null));
        FetchResponseData response = fetch(transform, MemoryRecords.withRecords(Compression.NONE,
                avro(KNOWN_SCHEMA_ID, "1"), avro(KNOWN_SCHEMA_ID, "o-2"), avro(KNOWN_SCHEMA_ID, "3"), avro(UNAVAILABLE_SCHEMA_ID, "4")));

        // when
        assertThatCode(() -> transform.onResponse(context, response))
                .withFailMessage(() -> "A registry outage escaped the fetch response, closing the broker connection")
                .doesNotThrowAnyException();

        // then
        assertStoppedRightAfterTheFirstRecord(partition(response, 0), "{\"n\":1}");
    }

    @Test
    void stopsAtAnEarlierRecordThatCannotBeTransformedRatherThanFailingOnALaterFilterEvaluationError() {
        // given a filtered topic under the fail policy whose mapping fails at offset 1 and whose filter cannot be evaluated at offset 2
        var transform = transform(new VirtualTopicConfig("orders.raw", new CelFilterConfig("int(value.n) > 0"), false,
                new JsonFormatConfig(DecodeErrorPolicy.FAIL), List.of(new CelTransformationConfig("{\"id\": value.order.id}")), null));
        FetchResponseData response = fetch(transform, MemoryRecords.withRecords(Compression.NONE,
                json("{\"n\":\"1\",\"order\":{\"id\":\"o-1\"}}"), json("{\"n\":\"1\",\"order\":5}"),
                json("{\"n\":\"not a number\",\"order\":{\"id\":\"o-3\"}}")));

        // when
        transform.onResponse(context, response);

        // then
        assertStoppedRightAfterTheFirstRecord(partition(response, 0), "{\"id\":\"o-1\"}");
    }

    @ParameterizedTest
    @EnumSource(DecodeErrorPolicy.class)
    void failsOnlyTheFilteredAndTransformedPartitionWhoseRecordsCannotBeFilteredDuringAnOutageWhateverThePolicy(DecodeErrorPolicy policy) {
        // given a filtered and transformed Avro topic
        var transform = transform(new VirtualTopicConfig("orders.raw", new CelFilterConfig("value == \"o-2\""), false,
                new AvroFormatConfig(policy), List.of(new ConvertTransformationConfig(new JsonFormatConfig())), null));
        FetchResponseData response = fetch(transform);

        // when
        assertThatCode(() -> transform.onResponse(context, response))
                .withFailMessage(() -> "A registry outage escaped the fetch response, closing the broker connection")
                .doesNotThrowAnyException();

        // then
        assertOutagePartitionFailed(response);
        assertThat(values(partition(response, 1).records()))
                .withFailMessage(() -> "The partition the registry could serve was not filtered and transformed as usual")
                .containsExactly("\"o-2\"");
    }

    @Test
    void answersUnknownServerErrorForAFilteredAndTransformedPartitionWhoseValuesCannotBeEncodedDuringAnOutage() {
        // given a filtered topic converting to Avro while the registry is down, under the fail policy
        var transform = transform(new VirtualTopicConfig("orders.raw", new CelFilterConfig("value.a > 0"), false,
                new JsonFormatConfig(DecodeErrorPolicy.FAIL),
                List.of(new ConvertTransformationConfig(new AvroFormatConfig(DecodeErrorPolicy.FAIL, "orders-value", null))), null),
                SchemaRegistries.of(null, (subject, version) -> {
                    throw new SchemaRegistryUnavailableException("schema registry is unreachable");
                }));
        FetchResponseData response = fetch(transform, MemoryRecords.withRecords(Compression.NONE, json("{\"a\":1}"), json("{\"a\":2}")));

        // when
        assertThatCode(() -> transform.onResponse(context, response))
                .withFailMessage(() -> "A registry outage escaped the fetch response, closing the broker connection")
                .doesNotThrowAnyException();

        // then
        FetchResponseData.PartitionData failed = partition(response, 0);
        assertThat(failed.errorCode())
                .withFailMessage(() -> "The partition hit by the outage did not fail with UNKNOWN_SERVER_ERROR, got " + Errors.forCode(failed.errorCode()))
                .isEqualTo(Errors.UNKNOWN_SERVER_ERROR.code());
        assertThat(size(failed.records()))
                .withFailMessage(() -> "The partition hit by the outage still delivered records, so the consumer would move past them")
                .isZero();
    }

    @Test
    void answersUnknownServerErrorForAFilteredAndTransformedPartitionWhoseFilterCannotBeEvaluated() {
        // given a filtered and transformed topic whose filter fails to evaluate on partition 0's valid record, under the fail policy
        var transform = transform(new VirtualTopicConfig("orders.raw", new CelFilterConfig("int(value.n) == 1"), false,
                new JsonFormatConfig(DecodeErrorPolicy.FAIL), List.of(new RenameFieldTransformationConfig("n", "m")), null));
        FetchResponseData response = fetch(transform,
                MemoryRecords.withRecords(Compression.NONE, json("{\"n\":\"not a number\"}"), json("{\"n\":\"1\"}")),
                MemoryRecords.withRecords(Compression.NONE, json("{\"n\":\"1\"}")));

        // when
        assertThatCode(() -> transform.onResponse(context, response))
                .withFailMessage(() -> "A filter evaluation error escaped the fetch response, closing the broker connection")
                .doesNotThrowAnyException();

        // then
        FetchResponseData.PartitionData failed = partition(response, 0);
        assertThat(failed.errorCode())
                .withFailMessage(() -> "The partition the filter failed on did not fail with UNKNOWN_SERVER_ERROR, got " + Errors.forCode(failed.errorCode()))
                .isEqualTo(Errors.UNKNOWN_SERVER_ERROR.code());
        assertThat(size(failed.records()))
                .withFailMessage(() -> "The partition the filter failed on still delivered records")
                .isZero();
        assertThat(values(partition(response, 1).records()))
                .withFailMessage(() -> "The other partition was not filtered and transformed as usual")
                .containsExactly("{\"m\":\"1\"}");
    }

    @Test
    void deliversACommitMarkerUnchangedOnAHeaderFilteredTopic() {
        // given a header-filtered topic, and a transaction whose only record the filter drops, followed by its commit marker
        var transform = transform(new VirtualTopicConfig("orders.raw", new HeaderEqualsFilterConfig("tenant", "acme")));
        FetchResponseData response = fetch(transform, concat(
                MemoryRecords.withTransactionalRecords(0L, Compression.NONE, 77L, (short) 3, 0, 5,
                        new SimpleRecord(1_000L, bytes("k"), bytes("{\"a\":1}"), new Header[]{new RecordHeader("tenant", bytes("other"))})),
                marker(1L, ControlRecordType.COMMIT)));

        // when
        transform.onResponse(context, response);

        // then
        assertMarkerDelivered(partition(response, 0), 1L, ControlRecordType.COMMIT);
    }

    @Test
    void deliversAnAbortMarkerOnACelFilteredTopicUnderSkipPolicy() {
        // given a CEL value-filtered JSON topic under the skip policy, and a transaction followed by its abort marker
        var transform = transform(new VirtualTopicConfig("orders.raw", new CelFilterConfig("value.a > 0"), false,
                new JsonFormatConfig(DecodeErrorPolicy.SKIP), List.of(), null));
        FetchResponseData response = fetch(transform, concat(
                MemoryRecords.withTransactionalRecords(0L, Compression.NONE, 77L, (short) 3, 0, 5, json("{\"a\":1}")),
                marker(1L, ControlRecordType.ABORT)));

        // when
        transform.onResponse(context, response);

        // then
        assertMarkerDelivered(partition(response, 0), 1L, ControlRecordType.ABORT);
    }

    @Test
    void deliversTheRecordsAfterACommitMarkerOnACelFilteredTopicUnderFailPolicy() {
        // given a CEL value-filtered JSON topic under the fail policy, and a commit marker at offset 1 between two records
        var transform = transform(new VirtualTopicConfig("orders.raw", new CelFilterConfig("value.a > 0"), false,
                new JsonFormatConfig(DecodeErrorPolicy.FAIL), List.of(), null));
        FetchResponseData response = fetch(transform, concat(
                MemoryRecords.withTransactionalRecords(0L, Compression.NONE, 77L, (short) 3, 0, 5, json("{\"a\":1}")),
                marker(1L, ControlRecordType.COMMIT),
                MemoryRecords.withRecords(2L, Compression.NONE, json("{\"a\":2}"))));

        // when
        transform.onResponse(context, response);

        // then
        FetchResponseData.PartitionData delivered = partition(response, 0);
        assertMarkerDelivered(delivered, 1L, ControlRecordType.COMMIT);
        assertThat(dataValues(delivered.records()))
                .withFailMessage(() -> "The partition stopped at the commit marker instead of delivering the record after it")
                .containsExactly("{\"a\":1}", "{\"a\":2}");
    }

    @Test
    void deliversACommitMarkerUnchangedOnAFilteredAndTransformedTopicUnderFailPolicy() {
        // given a filtered and transformed JSON topic under the fail policy, and a commit marker at offset 1 between two records
        var transform = transform(filteredRenamingTopic(DecodeErrorPolicy.FAIL));
        FetchResponseData response = fetch(transform, concat(
                MemoryRecords.withTransactionalRecords(0L, Compression.NONE, 77L, (short) 3, 0, 5, json("{\"a\":2}")),
                marker(1L, ControlRecordType.COMMIT),
                MemoryRecords.withRecords(2L, Compression.NONE, json("{\"a\":3}"))));

        // when
        transform.onResponse(context, response);

        // then
        FetchResponseData.PartitionData delivered = partition(response, 0);
        assertMarkerDelivered(delivered, 1L, ControlRecordType.COMMIT);
        assertThat(dataValues(delivered.records()))
                .withFailMessage(() -> "The records around the commit marker were not delivered filtered and transformed: " + dataValues(delivered.records()))
                .containsExactly("{\"b\":2}", "{\"b\":3}");
    }

    @Test
    void leavesOutALegacyBatchTheFilterDropsEntirelyAndDeliversTheRestOfThePartition() {
        // given a filtered JSON topic, a magic v1 batch at offset 0 the filter drops, and a batch at offset 1 it keeps
        var transform = transform(new VirtualTopicConfig("orders.raw", new CelFilterConfig("value.a > 1"), false,
                new JsonFormatConfig(), List.of(), null));
        FetchResponseData response = fetch(transform, concat(
                MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V1, 0L, Compression.NONE, TimestampType.CREATE_TIME, json("{\"a\":1}")),
                MemoryRecords.withRecords(1L, Compression.NONE, json("{\"a\":2}"))));

        // when
        transform.onResponse(context, response);

        // then
        assertLegacyBatchLeftOut(partition(response, 0), "{\"a\":2}");
    }

    @Test
    void leavesOutALegacyBatchTheFilterDropsEntirelyAndDeliversTheRestOfAFilteredAndTransformedPartition() {
        // given a filtered and transformed JSON topic, a magic v1 batch at offset 0 the filter drops, and a batch at offset 1 it keeps
        var transform = transform(filteredRenamingTopic(DecodeErrorPolicy.SKIP));
        FetchResponseData response = fetch(transform, concat(
                MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V1, 0L, Compression.NONE, TimestampType.CREATE_TIME, json("{\"a\":1}")),
                MemoryRecords.withRecords(1L, Compression.NONE, json("{\"a\":2}"))));

        // when
        transform.onResponse(context, response);

        // then
        assertLegacyBatchLeftOut(partition(response, 0), "{\"b\":2}");
    }

    @Test
    void deliversAPartlyFilteredLegacyBatchAsACurrentBatchAtTheOriginalOffsets() {
        // given a filtered and transformed JSON topic, and a compressed magic v1 batch at offsets 3-5 of which the filter keeps 4 and 5
        var transform = transform(filteredRenamingTopic(DecodeErrorPolicy.SKIP));
        FetchResponseData response = fetch(transform, MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V1, 3L, Compression.gzip().build(),
                TimestampType.CREATE_TIME, json("{\"a\":1}"), json("{\"a\":2}"), json("{\"a\":3}")));

        // when
        transform.onResponse(context, response);

        // then
        FetchResponseData.PartitionData delivered = partition(response, 0);
        assertThat(delivered.errorCode())
                .withFailMessage(() -> "The partly filtered legacy batch was not delivered without an error, got " + Errors.forCode(delivered.errorCode()))
                .isEqualTo(Errors.NONE.code());
        assertThat(batches(delivered.records())).extracting(RecordBatch::magic)
                .withFailMessage(() -> "The partly filtered legacy batch was not delivered as a current (v2) batch")
                .containsOnly(RecordBatch.CURRENT_MAGIC_VALUE);
        assertThat(offsets(delivered.records()))
                .withFailMessage(() -> "The kept legacy records did not keep their original offsets: " + offsets(delivered.records()))
                .containsExactly(4L, 5L);
        assertThat(values(delivered.records()))
                .withFailMessage(() -> "The kept legacy records were not transformed")
                .containsExactly("{\"b\":2}", "{\"b\":3}");
    }

    /// Fetches partition 0 of `orders` at `fetchOffset` (`null` leaves the partition out of the
    /// request, as an incremental fetch does when its offset did not change); the broker answers
    /// with the batch at offsets 1-7 whose record at 5 is not JSON.
    private FetchResponseData.PartitionData fetchAt(FetchVirtualTopicTransform transform, int requestSession, int epoch,
                                                    Long fetchOffset, int responseSession) {
        var fetchContext = new GatewayContext("consumer", System.nanoTime());
        var request = new FetchRequestData().setSessionId(requestSession).setSessionEpoch(epoch);
        if (fetchOffset != null) {
            request.topics().add(new FetchRequestData.FetchTopic().setTopic("orders").setPartitions(new ArrayList<>(List.of(
                    new FetchRequestData.FetchPartition().setPartition(0).setFetchOffset(fetchOffset)))));
        }
        transform.onRequest(fetchContext, request);

        var response = new FetchResponseData().setSessionId(responseSession);
        response.responses().add(new FetchResponseData.FetchableTopicResponse()
                .setTopic("orders.raw")
                .setPartitions(List.of(new FetchResponseData.PartitionData()
                        .setPartitionIndex(0)
                        .setHighWatermark(8)
                        .setRecords(MemoryRecords.withRecords(1L, Compression.NONE,
                                json("{\"a\":1}"), json("{\"a\":2}"), json("{\"a\":3}"), json("{\"a\":4}"),
                                json("not json"), json("{\"a\":6}"), json("{\"a\":7}"))))));
        transform.onResponse(fetchContext, response);
        return partition(response, 0);
    }

    /// Fetches partition 0 of the plain topic `payments` at `paymentsOffset` and partition 0 of `orders` at
    /// `ordersOffset` (`null` leaves a partition out of the request, as an incremental fetch does when its offset
    /// did not change); the broker answers, when `ordersInResponse`, with the batch of `orders.raw` at offsets 1-7
    /// whose record at 5 is not JSON, and with a record of `payments` when the request named it.
    private FetchResponseData fetchAlongsidePayments(FetchVirtualTopicTransform transform, int requestSession, int epoch,
                                                     Long paymentsOffset, Long ordersOffset, boolean ordersInResponse,
                                                     int responseSession) {
        var fetchContext = new GatewayContext("consumer", System.nanoTime());
        var request = new FetchRequestData().setSessionId(requestSession).setSessionEpoch(epoch);
        if (paymentsOffset != null) {
            request.topics().add(new FetchRequestData.FetchTopic().setTopic("payments").setPartitions(new ArrayList<>(List.of(
                    new FetchRequestData.FetchPartition().setPartition(0).setFetchOffset(paymentsOffset)))));
        }
        if (ordersOffset != null) {
            request.topics().add(new FetchRequestData.FetchTopic().setTopic("orders").setPartitions(new ArrayList<>(List.of(
                    new FetchRequestData.FetchPartition().setPartition(0).setFetchOffset(ordersOffset)))));
        }
        transform.onRequest(fetchContext, request);

        var response = new FetchResponseData().setSessionId(responseSession);
        if (paymentsOffset != null) {
            response.responses().add(new FetchResponseData.FetchableTopicResponse()
                    .setTopic("payments")
                    .setPartitions(new ArrayList<>(List.of(new FetchResponseData.PartitionData()
                            .setPartitionIndex(0)
                            .setHighWatermark(1)
                            .setRecords(MemoryRecords.withRecords(Compression.NONE, json("{\"p\":1}")))))));
        }
        if (ordersInResponse) {
            response.responses().add(new FetchResponseData.FetchableTopicResponse()
                    .setTopic("orders.raw")
                    .setPartitions(new ArrayList<>(List.of(new FetchResponseData.PartitionData()
                            .setPartitionIndex(0)
                            .setHighWatermark(8)
                            .setRecords(MemoryRecords.withRecords(1L, Compression.NONE,
                                    json("{\"a\":1}"), json("{\"a\":2}"), json("{\"a\":3}"), json("{\"a\":4}"),
                                    json("not json"), json("{\"a\":6}"), json("{\"a\":7}")))))));
        }
        transform.onResponse(fetchContext, response);
        return response;
    }

    /// Partition 0 of `orders`, whichever name the response gives the topic.
    private static FetchResponseData.PartitionData ordersPartition(FetchResponseData response) {
        return response.responses().stream()
                .filter(topic -> !topic.topic().equals("payments"))
                .findFirst()
                .orElseThrow()
                .partitions().getFirst();
    }

    private static List<Long> offsets(BaseRecords records) {
        List<Long> offsets = new ArrayList<>();
        for (Record record : ((MemoryRecords) records).records()) {
            offsets.add(record.offset());
        }
        return offsets;
    }

    private static long lastOffset(BaseRecords records) {
        long last = -1L;
        for (var batch : ((MemoryRecords) records).batches()) {
            last = batch.lastOffset();
        }
        return last;
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
        var virtualTopics = new VirtualTopicManager(Map.of("orders", orders), registries);
        return new FetchVirtualTopicTransform(virtualTopics, new FetchSessionRegistry());
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

    /// A JSON topic keeping the records whose `a` is above 1, with `a` renamed to `b`.
    private static VirtualTopicConfig filteredRenamingTopic(DecodeErrorPolicy policy) {
        return new VirtualTopicConfig("orders.raw", new CelFilterConfig("value.a > 1"), false, new JsonFormatConfig(policy),
                List.of(new RenameFieldTransformationConfig("a", "b")), null);
    }

    /// A JSON topic keeping the records whose `a` is above 0, mapped to `{"id": value.order.id}` - which fails on a
    /// record whose `order` is not an object.
    private static VirtualTopicConfig filteredMappingTopic(DecodeErrorPolicy policy) {
        return new VirtualTopicConfig("orders.raw", new CelFilterConfig("value.a > 0"), false, new JsonFormatConfig(policy),
                List.of(new CelTransformationConfig("{\"id\": value.order.id}")), null);
    }

    private FetchVirtualTopicTransform transform(VirtualTopicConfig orders, SchemaRegistries schemaRegistries) {
        var virtualTopics = new VirtualTopicManager(Map.of("orders", orders), schemaRegistries);
        return new FetchVirtualTopicTransform(virtualTopics, new FetchSessionRegistry());
    }

    /// Resolves schema id 1, uncached, counting every lookup in `lookups`; the registry is down for any other id.
    private static SchemaRegistries countingRegistries(AtomicInteger lookups) {
        return SchemaRegistries.of(id -> {
            lookups.incrementAndGet();
            if (id == KNOWN_SCHEMA_ID) {
                return Schema.create(Schema.Type.STRING);
            }
            throw new SchemaRegistryUnavailableException("schema registry is unreachable");
        }, null);
    }

    /// Fetches `orders`; the broker answers with `partition0` as its only partition.
    private FetchResponseData fetch(FetchVirtualTopicTransform transform, MemoryRecords partition0) {
        var request = new FetchRequestData();
        request.topics().add(new FetchRequestData.FetchTopic().setTopic("orders"));
        transform.onRequest(context, request);

        var response = new FetchResponseData();
        response.responses().add(new FetchResponseData.FetchableTopicResponse()
                .setTopic("orders.raw")
                .setPartitions(List.of(new FetchResponseData.PartitionData()
                        .setPartitionIndex(0)
                        .setHighWatermark(1)
                        .setRecords(partition0))));
        return response;
    }

    private static void assertStoppedRightAfterTheFirstRecord(FetchResponseData.PartitionData partition, String firstValue) {
        assertThat(partition.errorCode())
                .withFailMessage(() -> "The records before the earliest bad record were not delivered without an error, got "
                        + Errors.forCode(partition.errorCode()))
                .isEqualTo(Errors.NONE.code());
        assertThat(values(partition.records()))
                .withFailMessage(() -> "Only the record before the earliest bad record should have been delivered: " + values(partition.records()))
                .containsExactly(firstValue);
        assertThat(lastOffset(partition.records()))
                .withFailMessage(() -> "The delivered records do not end right before the earliest bad record, so the consumer would skip it")
                .isEqualTo(0L);
    }

    private static void assertMarkerDelivered(FetchResponseData.PartitionData partition, long offset, ControlRecordType type) {
        assertThat(partition.errorCode())
                .withFailMessage(() -> "The partition holding the " + type + " marker was not delivered without an error, got "
                        + Errors.forCode(partition.errorCode()))
                .isEqualTo(Errors.NONE.code());
        MutableRecordBatch marker = batches(partition.records()).stream()
                .filter(batch -> batch.baseOffset() == offset)
                .findFirst()
                .orElse(null);
        assertThat(marker)
                .withFailMessage(() -> "The " + type + " marker at offset " + offset + " was not delivered")
                .isNotNull();
        assertThat(marker.isControlBatch())
                .withFailMessage(() -> "The " + type + " marker is no longer a control batch")
                .isTrue();
        assertThat(marker.iterator().hasNext())
                .withFailMessage(() -> "The " + type + " marker was emptied")
                .isTrue();
        assertThat(ControlRecordType.parse(marker.iterator().next().key()))
                .withFailMessage(() -> "The " + type + " marker was changed")
                .isEqualTo(type);
    }

    private static void assertLegacyBatchLeftOut(FetchResponseData.PartitionData partition, String keptValue) {
        assertThat(partition.errorCode())
                .withFailMessage(() -> "The partition holding a fully filtered legacy batch was not delivered without an error, got "
                        + Errors.forCode(partition.errorCode()))
                .isEqualTo(Errors.NONE.code());
        assertThat(offsets(partition.records()))
                .withFailMessage(() -> "The fully filtered legacy batch was not left out, or the rest of the partition was not delivered: "
                        + offsets(partition.records()))
                .containsExactly(1L);
        assertThat(values(partition.records()))
                .withFailMessage(() -> "The rest of the partition was not delivered as usual")
                .containsExactly(keptValue);
    }

    private static MemoryRecords marker(long offset, ControlRecordType type) {
        return MemoryRecords.withEndTransactionMarker(offset, 5_000L, 5, 77L, (short) 3, new EndTransactionMarker(type, 0));
    }

    private static List<MutableRecordBatch> batches(BaseRecords records) {
        List<MutableRecordBatch> all = new ArrayList<>();
        ((MemoryRecords) records).batches().forEach(all::add);
        return all;
    }

    /// The records of the non-control batches of `records`.
    private static List<Record> dataRecords(BaseRecords records) {
        List<Record> all = new ArrayList<>();
        for (MutableRecordBatch batch : batches(records)) {
            if (!batch.isControlBatch()) {
                batch.forEach(all::add);
            }
        }
        return all;
    }

    private static List<String> dataValues(BaseRecords records) {
        return dataRecords(records).stream()
                .map(record -> StandardCharsets.UTF_8.decode(record.value()).toString())
                .toList();
    }

    private static MemoryRecords concat(MemoryRecords... parts) {
        int total = 0;
        for (MemoryRecords part : parts) {
            total += part.sizeInBytes();
        }
        ByteBuffer buffer = ByteBuffer.allocate(total);
        for (MemoryRecords part : parts) {
            buffer.put(part.buffer().duplicate());
        }
        buffer.flip();
        return MemoryRecords.readableRecords(buffer);
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    /// A string value in the schema registry wire format, with a `tenant` header.
    private static SimpleRecord avro(int schemaId, String value, String tenant) {
        SimpleRecord record = avro(schemaId, value);
        return new SimpleRecord(record.timestamp(), record.key(), record.value(), new Header[]{new RecordHeader("tenant", bytes(tenant))});
    }
}

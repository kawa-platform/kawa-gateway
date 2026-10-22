package io.jonasg.kawa.protocol.kafka;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaBodyCodecTest {

    private final KafkaApiRegistry registry = KafkaApiRegistry.create();
    private final KafkaBodyCodec codec = new KafkaBodyCodec(registry);

    private static <T> T first(Collection<T> collection) {
        return collection.iterator().next();
    }

    @Test
    void roundTripsProduceRequestWithRecords() {
        var request = new ProduceRequestData()
                .setAcks((short) 1)
                .setTimeoutMs(30000)
                .setTransactionalId(null);
        request.topicData().add(new ProduceRequestData.TopicProduceData().setName("orders-v2")
                .setPartitionData(List.of(new ProduceRequestData.PartitionProduceData()
                        .setIndex(0)
                        .setRecords(MemoryRecords.readableRecords(ByteBuffer.wrap(new byte[]{1, 2, 3, 4, 5}))))));

        ByteBuf buf = Unpooled.buffer();
        codec.encodeRequest((short) 8, request, buf);

        Object decoded = codec.decodeRequest(KafkaApiRegistry.PRODUCE, (short) 8, buf);
        ProduceRequestData data = (ProduceRequestData) decoded;
        assertThat(data.topicData()).hasSize(1);
        ProduceRequestData.TopicProduceData topic = first(data.topicData());
        assertThat(topic.name()).isEqualTo("orders-v2");
        MemoryRecords records = (MemoryRecords) first(topic.partitionData()).records();
        assertThat(records.buffer().remaining()).isEqualTo(5);
        assertThat(records.buffer().get(0)).isEqualTo((byte) 1);
    }

    @Test
    void roundTripsProduceRequestAtHighestVersion() {
        var request = new ProduceRequestData().setAcks((short) 1).setTimeoutMs(30000);
        request.topicData().add(new ProduceRequestData.TopicProduceData().setName("orders-v2")
                .setPartitionData(List.of(new ProduceRequestData.PartitionProduceData()
                        .setIndex(0)
                        .setRecords(MemoryRecords.readableRecords(ByteBuffer.wrap(new byte[]{1, 2, 3}))))));

        ByteBuf buf = Unpooled.buffer();
        codec.encodeRequest((short) 12, request, buf);

        ProduceRequestData data = (ProduceRequestData) codec.decodeRequest(KafkaApiRegistry.PRODUCE, (short) 12, buf);
        assertThat(first(data.topicData()).name())
                .withFailMessage(() -> "Produce v12 request did not decode the topic name")
                .isEqualTo("orders-v2");
    }

    @Test
    void roundTripsMetadataResponse() {
        var response = new MetadataResponseData();
        response.brokers().add(new MetadataResponseData.MetadataResponseBroker()
                .setNodeId(1).setHost("broker-1").setPort(9093).setRack(null));
        MetadataResponseData.MetadataResponseTopic topic =
                new MetadataResponseData.MetadataResponseTopic().setName("orders-v2")
                        .setErrorCode((short) 0)
                        .setIsInternal(false);
        topic.partitions().add(new MetadataResponseData.MetadataResponsePartition()
                .setPartitionIndex(0).setLeaderId(1)
                .setReplicaNodes(List.of(1)).setIsrNodes(List.of(1)).setOfflineReplicas(List.of()));
        response.topics().add(topic);

        ByteBuf buf = Unpooled.buffer();
        codec.encodeResponse((short) 8, response, buf);

        Object decoded = codec.decodeResponse(KafkaApiRegistry.METADATA, (short) 8, buf);
        MetadataResponseData data = (MetadataResponseData) decoded;
        assertThat(data.topics()).hasSize(1);
        MetadataResponseData.MetadataResponseTopic topicResp = first(data.topics());
        assertThat(topicResp.name()).isEqualTo("orders-v2");
        assertThat(first(topicResp.partitions()).leaderId()).isEqualTo(1);
        assertThat(first(data.brokers()).host()).isEqualTo("broker-1");
    }

    @Test
    void roundTripsApiVersionsRequest() {
        var request = new ApiVersionsRequestData()
                .setClientSoftwareName("kafka-client")
                .setClientSoftwareVersion("3.8.0");

        ByteBuf buf = Unpooled.buffer();
        codec.encodeRequest((short) 3, request, buf);

        Object decoded = codec.decodeRequest(KafkaApiRegistry.API_VERSIONS, (short) 3, buf);
        ApiVersionsRequestData data = (ApiVersionsRequestData) decoded;
        assertThat(data.clientSoftwareName()).isEqualTo("kafka-client");
        assertThat(data.clientSoftwareVersion()).isEqualTo("3.8.0");
    }

    @Test
    void roundTripsFetchRequest() {
        var request = new FetchRequestData()
                .setReplicaId(-1).setMaxWaitMs(500).setMinBytes(1).setMaxBytes(1_000_000)
                .setIsolationLevel((byte) 0).setSessionId(0).setSessionEpoch(-1);
        request.topics().add(new FetchRequestData.FetchTopic()
                .setTopic("orders-v2")
                .setPartitions(List.of(new FetchRequestData.FetchPartition()
                        .setPartition(0).setCurrentLeaderEpoch(-1).setFetchOffset(0L)
                        .setLastFetchedEpoch(-1).setLogStartOffset(0L).setPartitionMaxBytes(1_000_000))));

        ByteBuf buf = Unpooled.buffer();
        codec.encodeRequest((short) 11, request, buf);

        Object decoded = codec.decodeRequest(KafkaApiRegistry.FETCH, (short) 11, buf);
        FetchRequestData data = (FetchRequestData) decoded;
        assertThat(data.topics()).hasSize(1);
        FetchRequestData.FetchTopic topic = first(data.topics());
        assertThat(topic.topic()).isEqualTo("orders-v2");
        assertThat(first(topic.partitions()).partition()).isZero();
    }

    @Test
    void returnsNullForUnregisteredApi() {
        ByteBuf buf = Unpooled.buffer(0);

        // DescribeUserScramCredentials (50) is not registered
        assertThat(codec.decodeRequest((short) 50, (short) 1, buf)).isNull();
        assertThat(codec.decodeResponse((short) 50, (short) 1, buf)).isNull();
    }

    @Test
    void bodySizeMatchesEncodedBytes() {
        var response = new ProduceResponseData();
        response.responses().add(new ProduceResponseData.TopicProduceResponse()
                .setName("orders-v2")
                .setPartitionResponses(List.of(new ProduceResponseData.PartitionProduceResponse()
                        .setIndex(0).setErrorCode((short) 0).setBaseOffset(42L))));

        ByteBuf buf = Unpooled.buffer();
        codec.encodeResponse((short) 8, response, buf);

        assertThat(codec.bodySize((short) 8, response)).isEqualTo(buf.readableBytes());
    }

    @Test
    void addPartitionsToTxnTopLevelErrorCodeIsNotPresentOnVersionThree() {
        // The AddPartitionsToTxn response gained its top-level errorCode with the KIP-890
        // batched transaction format (v4+). kawa only decodes v0-3, so a denial cannot carry
        // the error on that top-level field - it must be expressed per topic/partition.
        var response = new AddPartitionsToTxnResponseData().setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code());

        ByteBuf buf = Unpooled.buffer();
        codec.encodeResponse((short) 3, response, buf);

        Object decoded = codec.decodeResponse(KafkaApiRegistry.ADD_PARTITIONS_TO_TXN, (short) 3, buf);
        AddPartitionsToTxnResponseData data = (AddPartitionsToTxnResponseData) decoded;
        assertThat(data.errorCode()).isZero();
    }
}

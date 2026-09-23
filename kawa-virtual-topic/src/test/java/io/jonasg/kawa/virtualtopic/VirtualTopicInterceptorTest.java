package io.jonasg.kawa.virtualtopic;

import io.jonasg.kawa.config.AdvertisedListener;
import io.jonasg.kawa.config.CelFilterConfig;
import io.jonasg.kawa.config.HeaderEqualsFilterConfig;
import io.jonasg.kawa.config.JsonFormatConfig;
import io.jonasg.kawa.config.VirtualTopicConfig;
import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.core.Request;
import io.jonasg.kawa.core.Response;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData;
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData;
import org.apache.kafka.common.message.AlterConfigsRequestData;
import org.apache.kafka.common.message.AlterConfigsResponseData;
import org.apache.kafka.common.message.CreateAclsRequestData;
import org.apache.kafka.common.message.CreateAclsResponseData;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.DeleteAclsRequestData;
import org.apache.kafka.common.message.DeleteAclsResponseData;
import org.apache.kafka.common.message.DeleteRecordsRequestData;
import org.apache.kafka.common.message.DeleteRecordsResponseData;
import org.apache.kafka.common.message.DescribeAclsRequestData;
import org.apache.kafka.common.message.DescribeAclsResponseData;
import org.apache.kafka.common.message.CreatePartitionsRequestData;
import org.apache.kafka.common.message.CreatePartitionsResponseData;
import org.apache.kafka.common.message.DeleteTopicsRequestData;
import org.apache.kafka.common.message.DeleteTopicsResponseData;
import org.apache.kafka.common.message.DescribeConfigsRequestData;
import org.apache.kafka.common.message.DescribeConfigsResponseData;
import org.apache.kafka.common.message.DescribeTransactionsRequestData;
import org.apache.kafka.common.message.DescribeTransactionsResponseData;
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData;
import org.apache.kafka.common.message.IncrementalAlterConfigsResponseData;
import org.apache.kafka.common.message.OffsetDeleteRequestData;
import org.apache.kafka.common.message.OffsetDeleteResponseData;
import org.apache.kafka.common.message.DescribeTopicPartitionsRequestData;
import org.apache.kafka.common.message.DescribeLogDirsRequestData;
import org.apache.kafka.common.message.DescribeLogDirsResponseData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData;
import org.apache.kafka.common.message.TxnOffsetCommitResponseData;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.assertj.core.api.Assertions.tuple;

class VirtualTopicInterceptorTest {

    private static final int METADATA = 3;
    private static final int PRODUCE = 0;
    private static final int FETCH = 1;
    private static final int LIST_OFFSETS = 2;
    private static final int OFFSET_COMMIT = 8;
    private static final int OFFSET_FETCH = 9;
    private static final int OFFSET_FOR_LEADER_EPOCH = 23;
    private static final int FIND_COORDINATOR = 10;
    private static final int CREATE_TOPICS = 19;
    private static final int CREATE_PARTITIONS = 37;
    private static final int DELETE_TOPICS = 20;
    private static final int DESCRIBE_LOG_DIRS = 35;
    private static final int DESCRIBE_TOPIC_PARTITIONS = 75;
    private static final int DESCRIBE_CONFIGS = 32;
    private static final int CREATE_ACLS = 30;
    private static final int ADD_PARTITIONS_TO_TXN = 24;
    private static final int OFFSET_DELETE = 47;
    private static final int ALTER_CONFIGS = 33;
    private static final int INCREMENTAL_ALTER_CONFIGS = 44;
    private static final int DELETE_RECORDS = 21;
    private static final int DELETE_ACLS = 31;
    private static final int DESCRIBE_ACLS = 29;
    private static final int DESCRIBE_TRANSACTIONS = 65;
    private static final int TXN_OFFSET_COMMIT = 28;

    private final VirtualTopicManager virtualTopics = new VirtualTopicManager(Map.of(
            "orders", new VirtualTopicConfig("orders-v2"),
            "customers", new VirtualTopicConfig("customers-v2"),
            "events", new VirtualTopicConfig("events-v2",
                    new HeaderEqualsFilterConfig("tenant", "acme")),
            "legacy", new VirtualTopicConfig("legacy-v1", null, true)));

    private final AdvertisedListener advertised = AdvertisedListener.of(1, "gateway.example.com", 9092);

    private final VirtualTopicInterceptor interceptor = new VirtualTopicInterceptor(virtualTopics, advertised);

    private final GatewayContext context = new GatewayContext("test", System.nanoTime());

    private static Request request(
            int apiKey,
            Object body
    ) {
        return new Request() {
            @Override
            public int apiKey() {
                return apiKey;
            }

            @Override
            public String apiName() {
                return "test";
            }

            @Override
            public short apiVersion() {
                return 8;
            }

            @Override
            public int correlationId() {
                return 1;
            }

            @Override
            public String clientId() {
                return "test-client";
            }

            @Override
            public Object body() {
                return body;
            }
        };
    }

    private static Response response(
            int apiKey,
            Object body
    ) {
        return new Response() {
            @Override
            public int apiKey() {
                return apiKey;
            }

            @Override
            public String apiName() {
                return "test";
            }

            @Override
            public short apiVersion() {
                return 8;
            }

            @Override
            public int correlationId() {
                return 1;
            }

            @Override
            public Object body() {
                return body;
            }
        };
    }

    @Test
    void rewritesMetadataRequestTopics() {
        var data = new MetadataRequestData();
        data.topics().add(new MetadataRequestData.MetadataRequestTopic().setName("orders"));
        data.topics().add(new MetadataRequestData.MetadataRequestTopic().setName("customers"));
        data.topics().add(new MetadataRequestData.MetadataRequestTopic().setName("unmapped"));

        interceptor.onRequest(context, request(METADATA, data));

        assertThat(data.topics()).extracting("name")
                .containsExactly("orders-v2", "customers-v2", "unmapped");
    }

    @Test
    void rejectCreatingVirtualTopics() {
        var requestData = new CreateTopicsRequestData();
        requestData.topics().add(new CreateTopicsRequestData.CreatableTopic()
                .setName("orders")
                .setNumPartitions(1)
                .setReplicationFactor((short) 1));

        GatewayContext requestContext = freshContext();
        interceptor.onRequest(requestContext, request(CREATE_TOPICS, requestData));

        assertThat(requestContext.state(VirtualTopicState.class)).isInstanceOf(VirtualTopicState.class);
        Object shortCircuit = requestContext.shortCircuitResult();
        assertThat(shortCircuit).isNotNull();

        Object body = invokeNoArg(shortCircuit, "body");
        assertThat(body).isInstanceOf(CreateTopicsResponseData.class);
        CreateTopicsResponseData responseData = (CreateTopicsResponseData) body;
        assertThat(responseData.topics()).hasSize(1);

        CreateTopicsResponseData.CreatableTopicResult topicResult = responseData.topics().iterator().next();
        assertThat(topicResult.name()).isEqualTo("orders");
        assertThat(topicResult.errorCode()).isEqualTo(Errors.INVALID_REQUEST.code());
        assertThat(topicResult.errorMessage())
                .contains("orders")
                .contains("orders-v2");
    }

    @Test
    void physicalCreateTopicsPassesThrough() {
        var requestData = new CreateTopicsRequestData();
        requestData.topics().add(new CreateTopicsRequestData.CreatableTopic()
                .setName("orders-v2")
                .setNumPartitions(1)
                .setReplicationFactor((short) 1));

        GatewayContext requestContext = freshContext();
        interceptor.onRequest(requestContext, request(CREATE_TOPICS, requestData));

        assertThat(requestData.topics()).extracting("name").containsExactly("orders-v2");
        Object shortCircuit = requestContext.shortCircuitResult();
        assertThat(shortCircuit).isNull();
    }

    @Test
    void mixedCreateTopicsRejectsAliasesAndForwardsPhysical() {
        var requestData = new CreateTopicsRequestData();
        requestData.topics().add(new CreateTopicsRequestData.CreatableTopic()
                .setName("orders")
                .setNumPartitions(1)
                .setReplicationFactor((short) 1));
        requestData.topics().add(new CreateTopicsRequestData.CreatableTopic()
                .setName("new-topic")
                .setNumPartitions(1)
                .setReplicationFactor((short) 1));

        GatewayContext requestContext = freshContext();
        interceptor.onRequest(requestContext, request(CREATE_TOPICS, requestData));

        // alias dropped from the forwarded request, physical topic kept
        assertThat(requestData.topics()).extracting("name").containsExactly("new-topic");
        // no short-circuit — the filtered request is forwarded
        Object shortCircuit = requestContext.shortCircuitResult();
        assertThat(shortCircuit).isNull();

        // broker responds for the forwarded topic
        var responseData = new CreateTopicsResponseData();
        responseData.topics().add(new CreateTopicsResponseData.CreatableTopicResult()
                .setName("new-topic")
                .setErrorCode((short) 0));
        interceptor.onResponse(requestContext, response(CREATE_TOPICS, responseData));

        // local error for the alias merged into the broker response
        assertThat(responseData.topics()).extracting("name")
                .containsExactly("new-topic", "orders");
        CreateTopicsResponseData.CreatableTopicResult ordersResult = responseData.topics().stream()
                .filter(r -> r.name().equals("orders"))
                .findFirst().orElseThrow();
        assertThat(ordersResult.errorCode()).isEqualTo(Errors.INVALID_REQUEST.code());
        assertThat(ordersResult.errorMessage())
                .contains("orders")
                .contains("orders-v2");
    }

    @Test
    void rejectDeletingVirtualTopics() {
        var requestData = new DeleteTopicsRequestData();
        requestData.topicNames().add("orders");

        GatewayContext requestContext = freshContext();
        interceptor.onRequest(requestContext, request(DELETE_TOPICS, requestData));

        assertThat(requestData.topicNames()).isEmpty();
        Object shortCircuit = requestContext.shortCircuitResult();
        assertThat(shortCircuit).isNotNull();

        Object body = invokeNoArg(shortCircuit, "body");
        assertThat(body).isInstanceOf(DeleteTopicsResponseData.class);
        DeleteTopicsResponseData responseData = (DeleteTopicsResponseData) body;
        assertThat(responseData.responses()).hasSize(1);

        DeleteTopicsResponseData.DeletableTopicResult topicResult = responseData.responses().iterator().next();
        assertThat(topicResult.name()).isEqualTo("orders");
        assertThat(topicResult.errorCode()).isEqualTo(Errors.INVALID_REQUEST.code());
        assertThat(topicResult.errorMessage())
                .contains("orders")
                .contains("orders-v2");
    }

    @Test
    void physicalDeleteTopicsPassesThrough() {
        var requestData = new DeleteTopicsRequestData();
        requestData.topics().add(new DeleteTopicsRequestData.DeleteTopicState().setName("orders-v2"));

        GatewayContext requestContext = freshContext();
        interceptor.onRequest(requestContext, request(DELETE_TOPICS, requestData));

        assertThat(requestData.topics()).extracting("name").containsExactly("orders-v2");
        Object shortCircuit = requestContext.shortCircuitResult();
        assertThat(shortCircuit).isNull();
    }

    @Test
    void mixedDeleteTopicsRejectsAliasesAndForwardsPhysical() {
        var requestData = new DeleteTopicsRequestData();
        requestData.topics().add(new DeleteTopicsRequestData.DeleteTopicState().setName("orders"));
        requestData.topics().add(new DeleteTopicsRequestData.DeleteTopicState().setName("new-topic"));

        GatewayContext requestContext = freshContext();
        interceptor.onRequest(requestContext, request(DELETE_TOPICS, requestData));

        assertThat(requestData.topics()).extracting("name").containsExactly("new-topic");
        Object shortCircuit = requestContext.shortCircuitResult();
        assertThat(shortCircuit).isNull();

        var responseData = new DeleteTopicsResponseData();
        responseData.responses().add(new DeleteTopicsResponseData.DeletableTopicResult()
                .setName("new-topic")
                .setErrorCode((short) 0));
        interceptor.onResponse(requestContext, response(DELETE_TOPICS, responseData));

        assertThat(responseData.responses()).extracting("name")
                .containsExactly("new-topic", "orders");
        DeleteTopicsResponseData.DeletableTopicResult ordersResult = responseData.responses().stream()
                .filter(r -> r.name().equals("orders"))
                .findFirst().orElseThrow();
        assertThat(ordersResult.errorCode()).isEqualTo(Errors.INVALID_REQUEST.code());
        assertThat(ordersResult.errorMessage())
                .contains("orders")
                .contains("orders-v2");
    }

    @Test
    void rewritesMetadataResponseTopicsAndBrokers() {
        var data = new MetadataResponseData();
        data.brokers().add(new MetadataResponseData.MetadataResponseBroker()
                .setNodeId(0).setHost("broker-internal").setPort(9093).setRack(null));
        data.brokers().add(new MetadataResponseData.MetadataResponseBroker()
                .setNodeId(1).setHost("broker-internal-2").setPort(9093).setRack(null));
        addTopic(data, "orders-v2");
        addTopic(data, "customers-v2");
        addTopic(data, "internal-hidden-topic");

        interceptor.onResponse(context, response(METADATA, data));

        // physical topics backing a virtual topic are hidden: renamed to their virtual name in
        // place rather than exposed alongside it. Non-virtual topics pass through untouched.
        assertThat(data.topics()).extracting("name")
                .containsExactly("orders", "customers", "internal-hidden-topic");
        for (MetadataResponseData.MetadataResponseBroker broker : data.brokers()) {
            assertThat(broker.host()).isEqualTo("gateway.example.com");
            assertThat(broker.port()).isEqualTo(9092);
        }
    }

    private static void addTopic(
            MetadataResponseData data,
            String name
    ) {
        MetadataResponseData.MetadataResponseTopic topic =
                new MetadataResponseData.MetadataResponseTopic().setName(name)
                        .setErrorCode((short) 0).setIsInternal(false);
        topic.partitions().add(new MetadataResponseData.MetadataResponsePartition()
                .setPartitionIndex(0).setLeaderId(1)
                .setReplicaNodes(List.of(1)).setIsrNodes(List.of(1)).setOfflineReplicas(List.of()));
        data.topics().add(topic);
    }

    @Test
    void rewritesProduceRequestAndResponse() {
        var data = new ProduceRequestData();
        data.topicData().add(new ProduceRequestData.TopicProduceData().setName("orders"));

        interceptor.onRequest(context, request(PRODUCE, data));
        assertThat(first(data.topicData()).name()).isEqualTo("orders-v2");

        var responseData = new ProduceResponseData();
        responseData.responses().add(new ProduceResponseData.TopicProduceResponse().setName("orders-v2"));

        interceptor.onResponse(context, response(PRODUCE, responseData));
        assertThat(first(responseData.responses()).name()).isEqualTo("orders");
    }

    @Test
    void rewritesFetchRequestAndResponse() {
        var data = new FetchRequestData();
        data.topics().add(new FetchRequestData.FetchTopic().setTopic("customers"));

        interceptor.onRequest(context, request(FETCH, data));
        assertThat(first(data.topics()).topic()).isEqualTo("customers-v2");

        var responseData = new FetchResponseData();
        responseData.responses().add(new FetchResponseData.FetchableTopicResponse().setTopic("customers-v2"));

        interceptor.onResponse(context, response(FETCH, responseData));
        assertThat(first(responseData.responses()).topic()).isEqualTo("customers");
    }

    @Test
    void filtersFetchResponseRecordsByHeader() {
        // given a fetch request for the virtual topic, binding the per-request mapping
        var requestData = new FetchRequestData();
        requestData.topics().add(new FetchRequestData.FetchTopic().setTopic("events"));
        interceptor.onRequest(context, request(FETCH, requestData));
        assertThat(first(requestData.topics()).topic()).isEqualTo("events-v2");

        var matching = new SimpleRecord(
                1000L, "k1".getBytes(StandardCharsets.UTF_8), "v1".getBytes(StandardCharsets.UTF_8),
                new Header[]{new RecordHeader("tenant", "acme".getBytes(StandardCharsets.UTF_8))});
        var nonMatching = new SimpleRecord(
                2000L, "k2".getBytes(StandardCharsets.UTF_8), "v2".getBytes(StandardCharsets.UTF_8),
                new Header[]{new RecordHeader("tenant", "other".getBytes(StandardCharsets.UTF_8))});
        var records = MemoryRecords.withRecords(Compression.NONE, matching, nonMatching);

        var responseData = new FetchResponseData();
        responseData.responses().add(new FetchResponseData.FetchableTopicResponse()
                .setTopic("events-v2")
                .setPartitions(List.of(new FetchResponseData.PartitionData()
                        .setPartitionIndex(0)
                        .setRecords(records))));

        interceptor.onResponse(context, response(FETCH, responseData));

        assertThat(first(responseData.responses()).topic()).isEqualTo("events");

        var filtered = (MemoryRecords) first(responseData.responses()).partitions().get(0).records();
        List<Record> survivors = new ArrayList<>();
        filtered.records().forEach(survivors::add);

        assertThat(survivors).hasSize(1);
        assertThat(survivors.get(0).offset()).isEqualTo(0L);
        assertThat(StandardCharsets.UTF_8.decode(survivors.get(0).key()).toString()).isEqualTo("k1");
    }

    @Test
    void unfilteredFetchResponseRecordsPassThroughUntouched() {
        var record = new SimpleRecord(
                1000L,
                "k1".getBytes(StandardCharsets.UTF_8),
                "v1".getBytes(StandardCharsets.UTF_8));
        var records = MemoryRecords.withRecords(Compression.NONE, record);

        var responseData = new FetchResponseData();
        responseData.responses().add(new FetchResponseData.FetchableTopicResponse()
                .setTopic("customers-v2")
                .setPartitions(List.of(new FetchResponseData.PartitionData()
                        .setPartitionIndex(0)
                        .setRecords(records))));

        interceptor.onResponse(context, response(FETCH, responseData));

        assertThat(first(responseData.responses()).partitions().get(0).records()).isSameAs(records);
    }

    @Test
    void filteredFetchResponseKeepsOriginalOffsetsOfSurvivors() {
        // given a batch whose FIRST record does not match the filter
        var nonMatching = new SimpleRecord(
                1000L, "k1".getBytes(StandardCharsets.UTF_8), "v1".getBytes(StandardCharsets.UTF_8),
                new Header[]{new RecordHeader("tenant", "other".getBytes(StandardCharsets.UTF_8))});
        var matching = new SimpleRecord(
                2000L, "k2".getBytes(StandardCharsets.UTF_8), "v2".getBytes(StandardCharsets.UTF_8),
                new Header[]{new RecordHeader("tenant", "acme".getBytes(StandardCharsets.UTF_8))});
        var records = MemoryRecords.withRecords(Compression.NONE, nonMatching, matching);

        var responseData = new FetchResponseData();
        responseData.responses().add(new FetchResponseData.FetchableTopicResponse()
                .setTopic("events-v2")
                .setPartitions(List.of(new FetchResponseData.PartitionData()
                        .setPartitionIndex(0)
                        .setRecords(records))));

        // when the response is filtered
        interceptor.onResponse(context, response(FETCH, responseData));

        // then the survivor keeps its original offset 1 instead of being renumbered to 0
        var filtered = (MemoryRecords) first(responseData.responses()).partitions().get(0).records();
        List<Record> survivors = new ArrayList<>();
        filtered.records().forEach(survivors::add);

        assertThat(survivors).hasSize(1);
        assertThat(survivors.get(0).offset())
                .describedAs("surviving records must keep their original offsets")
                .isEqualTo(1L);
    }

    @Test
    void fullyFilteredFetchResponseStillReturnsValidEmptyRecords() {
        // given a partition whose only record does not match the filter
        var nonMatching = new SimpleRecord(
                1000L, "k1".getBytes(StandardCharsets.UTF_8), "v1".getBytes(StandardCharsets.UTF_8),
                new Header[]{new RecordHeader("tenant", "other".getBytes(StandardCharsets.UTF_8))});
        var records = MemoryRecords.withRecords(Compression.NONE, nonMatching);

        var responseData = new FetchResponseData();
        responseData.responses().add(new FetchResponseData.FetchableTopicResponse()
                .setTopic("events-v2")
                .setPartitions(List.of(new FetchResponseData.PartitionData()
                        .setPartitionIndex(0)
                        .setRecords(records))));

        // when the response is filtered
        interceptor.onResponse(context, response(FETCH, responseData));

        // then the partition still carries a valid (readable) records payload with no records
        var filtered = first(responseData.responses()).partitions().get(0).records();
        assertThat(filtered).isNotNull().isInstanceOf(MemoryRecords.class);
        List<Record> survivors = new ArrayList<>();
        ((MemoryRecords) filtered).records().forEach(survivors::add);
        assertThat(survivors).isEmpty();
    }

    @Test
    void filteredFetchResponseLeavesOffsetMetadataUntouched() {
        // given a filtered partition carrying offset metadata
        var matching = new SimpleRecord(
                1000L, "k1".getBytes(StandardCharsets.UTF_8), "v1".getBytes(StandardCharsets.UTF_8),
                new Header[]{new RecordHeader("tenant", "acme".getBytes(StandardCharsets.UTF_8))});
        var records = MemoryRecords.withRecords(Compression.NONE, matching);

        var responseData = new FetchResponseData();
        responseData.responses().add(new FetchResponseData.FetchableTopicResponse()
                .setTopic("events-v2")
                .setPartitions(List.of(new FetchResponseData.PartitionData()
                        .setPartitionIndex(0)
                        .setHighWatermark(50L)
                        .setLastStableOffset(49L)
                        .setLogStartOffset(0L)
                        .setRecords(records))));

        // when the response is filtered
        interceptor.onResponse(context, response(FETCH, responseData));

        // then the partition's offset metadata is untouched by the filtering
        FetchResponseData.PartitionData partition =
                first(responseData.responses()).partitions().get(0);
        assertThat(partition.highWatermark()).isEqualTo(50L);
        assertThat(partition.lastStableOffset()).isEqualTo(49L);
        assertThat(partition.logStartOffset()).isZero();
    }

    @Test
    void filtersIdleIncrementalFetchResponsesViaSession() {
        // given a fetch session bound on a full fetch for the filtered virtual topic
        var sessions = new FetchSessionRegistry();
        var sessionInterceptor = new VirtualTopicInterceptor(virtualTopics, advertised, sessions);

        var create = new FetchRequestData();
        create.topics().add(new FetchRequestData.FetchTopic().setTopic("events"));
        GatewayContext createContext = freshContext();
        sessionInterceptor.onRequest(createContext, request(FETCH, create));
        assertThat(first(create.topics()).topic()).isEqualTo("events-v2");

        var createdResponse = new FetchResponseData().setSessionId(42);
        createdResponse.responses().add(new FetchResponseData.FetchableTopicResponse().setTopic("events-v2"));
        sessionInterceptor.onResponse(createContext, response(FETCH, createdResponse));
        assertThat(first(createdResponse.responses()).topic()).isEqualTo("events");
        assertThat(sessions.hasSession("test", 42)).isTrue();

        // when an idle incremental fetch response carries records for the physical topic
        var incremental = new FetchRequestData().setSessionId(42).setSessionEpoch(1);
        sessionInterceptor.onRequest(freshContext(), request(FETCH, incremental));

        var matching = new SimpleRecord(
                1000L, "k1".getBytes(StandardCharsets.UTF_8), "v1".getBytes(StandardCharsets.UTF_8),
                new Header[]{new RecordHeader("tenant", "acme".getBytes(StandardCharsets.UTF_8))});
        var nonMatching = new SimpleRecord(
                2000L, "k2".getBytes(StandardCharsets.UTF_8), "v2".getBytes(StandardCharsets.UTF_8),
                new Header[]{new RecordHeader("tenant", "other".getBytes(StandardCharsets.UTF_8))});
        var incrementalResponse = new FetchResponseData().setSessionId(42);
        incrementalResponse.responses().add(new FetchResponseData.FetchableTopicResponse()
                .setTopic("events-v2")
                .setPartitions(List.of(new FetchResponseData.PartitionData()
                        .setPartitionIndex(0)
                        .setRecords(MemoryRecords.withRecords(Compression.NONE, matching, nonMatching)))));
        sessionInterceptor.onResponse(freshContext(), response(FETCH, incrementalResponse));

        // then the topic is renamed via the session AND its records are filtered
        assertThat(first(incrementalResponse.responses()).topic()).isEqualTo("events");
        var filtered = (MemoryRecords) first(incrementalResponse.responses()).partitions().get(0).records();
        List<Record> survivors = new ArrayList<>();
        filtered.records().forEach(survivors::add);
        assertThat(survivors).hasSize(1);
        assertThat(StandardCharsets.UTF_8.decode(survivors.get(0).key()).toString()).isEqualTo("k1");
    }

    @Test
    void rewritesDescribeTopicPartitionsRequest() {
        var data = new DescribeTopicPartitionsRequestData();
        data.topics().add(new DescribeTopicPartitionsRequestData.TopicRequest().setName("orders"));

        interceptor.onRequest(context, request(DESCRIBE_TOPIC_PARTITIONS, data));

        assertThat(first(data.topics()).name()).isEqualTo("orders-v2");
    }

    @Test
    void rewritesDescribeLogDirsRequestAndResponse() {
        // given
        var requestData = new DescribeLogDirsRequestData();
        requestData.topics().add(new DescribeLogDirsRequestData.DescribableLogDirTopic().setTopic("orders"));

        // when
        interceptor.onRequest(context, request(DESCRIBE_LOG_DIRS, requestData));

        // then
        assertThat(first(requestData.topics()).topic()).isEqualTo("orders-v2");

        // given
        var responseData = new DescribeLogDirsResponseData();
        responseData.results().add(new DescribeLogDirsResponseData.DescribeLogDirsResult()
                .setLogDir("/kafka/logs")
                .setErrorCode((short) 0)
                .setTopics(List.of(new DescribeLogDirsResponseData.DescribeLogDirsTopic()
                        .setName("orders-v2"))));

        // when
        interceptor.onResponse(context, response(DESCRIBE_LOG_DIRS, responseData));

        // then
        assertThat(first(first(responseData.results()).topics()).name()).isEqualTo("orders");
    }

    @Test
    void rewritesOffsetForLeaderEpochRequestAndResponse() {
        // given
        var requestData = new OffsetForLeaderEpochRequestData();
        requestData.topics().add(new OffsetForLeaderEpochRequestData.OffsetForLeaderTopic().setTopic("orders"));

        // when
        interceptor.onRequest(context, request(OFFSET_FOR_LEADER_EPOCH, requestData));

        // then
        assertThat(first(requestData.topics()).topic()).isEqualTo("orders-v2");

        // given
        var responseData = new OffsetForLeaderEpochResponseData();
        responseData.topics().add(new OffsetForLeaderEpochResponseData.OffsetForLeaderTopicResult()
                .setTopic("orders-v2"));

        // when
        interceptor.onResponse(context, response(OFFSET_FOR_LEADER_EPOCH, responseData));

        // then
        assertThat(first(responseData.topics()).topic()).isEqualTo("orders");
    }

    @Test
    void rewritesListOffsetsRequestAndResponse() {
        var data = new ListOffsetsRequestData();
        data.topics().add(new ListOffsetsRequestData.ListOffsetsTopic().setName("orders"));

        interceptor.onRequest(context, request(LIST_OFFSETS, data));
        assertThat(first(data.topics()).name()).isEqualTo("orders-v2");

        var responseData = new ListOffsetsResponseData();
        responseData.topics().add(new ListOffsetsResponseData.ListOffsetsTopicResponse().setName("orders-v2"));

        interceptor.onResponse(context, response(LIST_OFFSETS, responseData));
        assertThat(first(responseData.topics()).name()).isEqualTo("orders");
    }

    @Test
    void rewritesOffsetCommitRequestAndResponse() {
        var data = new OffsetCommitRequestData().setGroupId("g1");
        data.topics().add(new OffsetCommitRequestData.OffsetCommitRequestTopic().setName("orders"));

        interceptor.onRequest(context, request(OFFSET_COMMIT, data));
        assertThat(first(data.topics()).name()).isEqualTo("orders-v2");

        var responseData = new OffsetCommitResponseData();
        responseData.topics().add(new OffsetCommitResponseData.OffsetCommitResponseTopic().setName("orders-v2"));

        interceptor.onResponse(context, response(OFFSET_COMMIT, responseData));
        assertThat(first(responseData.topics()).name()).isEqualTo("orders");
    }

    @Test
    void rewritesTxnOffsetCommitRequestAndResponse() {
        var data = new TxnOffsetCommitRequestData()
                .setTransactionalId("txn-1")
                .setGroupId("g1")
                .setProducerId(42L)
                .setProducerEpoch((short) 7);
        data.topics().add(new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic().setName("orders"));

        interceptor.onRequest(context, request(TXN_OFFSET_COMMIT, data));
        assertThat(first(data.topics()).name()).isEqualTo("orders-v2");
        assertThat(data.transactionalId()).isEqualTo("txn-1");
        assertThat(data.producerId()).isEqualTo(42L);
        assertThat(data.producerEpoch()).isEqualTo((short) 7);

        var responseData = new TxnOffsetCommitResponseData();
        responseData.topics().add(new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic().setName("orders-v2"));

        interceptor.onResponse(context, response(TXN_OFFSET_COMMIT, responseData));
        assertThat(first(responseData.topics()).name()).isEqualTo("orders");
    }

    @Test
    void rewritesOffsetFetchRequestAndResponse() {
        var data = new OffsetFetchRequestData().setGroupId("g1");
        data.topics().add(new OffsetFetchRequestData.OffsetFetchRequestTopic().setName("orders"));

        interceptor.onRequest(context, request(OFFSET_FETCH, data));
        assertThat(first(data.topics()).name()).isEqualTo("orders-v2");

        var responseData = new OffsetFetchResponseData();
        responseData.topics().add(new OffsetFetchResponseData.OffsetFetchResponseTopic().setName("orders-v2"));

        interceptor.onResponse(context, response(OFFSET_FETCH, responseData));
        assertThat(first(responseData.topics()).name()).isEqualTo("orders");
    }

    @Test
    void nullOffsetFetchTopicsIsIgnored() {
        var data = new OffsetFetchRequestData().setGroupId("g1");
        data.setTopics(null);

        interceptor.onRequest(context, request(OFFSET_FETCH, data));
    }

    @Test
    void offsetFetchAllTopicsResponseShouldStillUseVirtualNames() {
        var requestData = new OffsetFetchRequestData().setGroupId("g1");
        requestData.setTopics(null);

        GatewayContext requestContext = freshContext();
        interceptor.onRequest(requestContext, request(OFFSET_FETCH, requestData));

        var responseData = new OffsetFetchResponseData();
        responseData.topics().add(new OffsetFetchResponseData.OffsetFetchResponseTopic().setName("orders-v2"));

        interceptor.onResponse(requestContext, response(OFFSET_FETCH, responseData));

        assertThat(first(responseData.topics()).name()).isEqualTo("orders");
    }

    @Test
    void responsesKeepPhysicalNamesWhenRequestSpokePhysical() {
        var listOffsetsResponse = new ListOffsetsResponseData();
        listOffsetsResponse.topics().add(new ListOffsetsResponseData.ListOffsetsTopicResponse().setName("orders-v2"));
        interceptor.onResponse(freshContext(), response(LIST_OFFSETS, listOffsetsResponse));
        assertThat(first(listOffsetsResponse.topics()).name()).isEqualTo("orders-v2");

        var fetchResponse = new FetchResponseData();
        fetchResponse.responses().add(new FetchResponseData.FetchableTopicResponse().setTopic("orders-v2"));
        interceptor.onResponse(freshContext(), response(FETCH, fetchResponse));
        assertThat(first(fetchResponse.responses()).topic()).isEqualTo("orders-v2");

        var commitResponse = new OffsetCommitResponseData();
        commitResponse.topics().add(new OffsetCommitResponseData.OffsetCommitResponseTopic().setName("orders-v2"));
        interceptor.onResponse(freshContext(), response(OFFSET_COMMIT, commitResponse));
        assertThat(first(commitResponse.topics()).name()).isEqualTo("orders-v2");

        var offsetFetchResponse = new OffsetFetchResponseData();
        offsetFetchResponse.topics().add(new OffsetFetchResponseData.OffsetFetchResponseTopic().setName("orders-v2"));
        interceptor.onResponse(freshContext(), response(OFFSET_FETCH, offsetFetchResponse));
        assertThat(first(offsetFetchResponse.topics()).name()).isEqualTo("orders-v2");
    }

    @Test
    void rewritesCreatePartitionsRequestAndResponse() {
        var data = new CreatePartitionsRequestData();
        data.topics().add(new CreatePartitionsRequestData.CreatePartitionsTopic()
                .setName("orders")
                .setCount(3)
                .setAssignments(List.of(
                        new CreatePartitionsRequestData.CreatePartitionsAssignment().setBrokerIds(List.of(1, 2)),
                        new CreatePartitionsRequestData.CreatePartitionsAssignment().setBrokerIds(List.of(2, 3)))));
        data.topics().add(new CreatePartitionsRequestData.CreatePartitionsTopic()
                .setName("unmapped")
                .setCount(2));

        interceptor.onRequest(context, request(CREATE_PARTITIONS, data));

        assertThat(data.topics()).extracting("name", "count")
                .containsExactly(tuple("orders-v2", 3), tuple("unmapped", 2));
        assertThat(first(data.topics()).assignments()).hasSize(2);
        assertThat(first(first(data.topics()).assignments()).brokerIds()).containsExactly(1, 2);

        var responseData = new CreatePartitionsResponseData();
        responseData.results().add(new CreatePartitionsResponseData.CreatePartitionsTopicResult()
                .setName("orders-v2")
                .setErrorCode((short) 0));

        interceptor.onResponse(context, response(CREATE_PARTITIONS, responseData));

        assertThat(first(responseData.results()).name()).isEqualTo("orders");
    }

    @Test
    void createPartitionsPhysicalNamesPassThrough() {
        var data = new CreatePartitionsRequestData();
        data.topics().add(new CreatePartitionsRequestData.CreatePartitionsTopic()
                .setName("orders-v2")
                .setCount(3));

        interceptor.onRequest(context, request(CREATE_PARTITIONS, data));

        assertThat(first(data.topics()).name()).isEqualTo("orders-v2");
        assertThat(VirtualTopicState.from(context).physicalToVirtual()).isEmpty();
    }

    @Test
    void rewritesDescribeConfigsTopicResourcesRequestAndResponse() {
        // given a describe-configs request for the virtual topic "orders"
        var requestData = new DescribeConfigsRequestData();
        requestData.resources().add(new DescribeConfigsRequestData.DescribeConfigsResource()
                .setResourceType(ConfigResource.Type.TOPIC.id())
                .setResourceName("orders")
                .setConfigurationKeys(List.of("retention.ms", "cleanup.policy")));

        // when
        interceptor.onRequest(context, request(DESCRIBE_CONFIGS, requestData));

        // then the topic resource is rewritten to the physical name, keys preserved
        DescribeConfigsRequestData.DescribeConfigsResource resource = first(requestData.resources());
        assertThat(resource.resourceName()).isEqualTo("orders-v2");
        assertThat(resource.resourceType()).isEqualTo(ConfigResource.Type.TOPIC.id());
        assertThat(resource.configurationKeys()).containsExactly("retention.ms", "cleanup.policy");

        // given a broker response naming the physical topic
        var responseData = new DescribeConfigsResponseData();
        responseData.results().add(new DescribeConfigsResponseData.DescribeConfigsResult()
                .setResourceType(ConfigResource.Type.TOPIC.id())
                .setResourceName("orders-v2")
                .setErrorCode((short) 0)
                .setConfigs(List.of(new DescribeConfigsResponseData.DescribeConfigsResourceResult()
                        .setName("retention.ms").setValue("100000"))));

        // when
        interceptor.onResponse(context, response(DESCRIBE_CONFIGS, responseData));

        // then the result carries the virtual name and its config entries back to the client
        DescribeConfigsResponseData.DescribeConfigsResult result = first(responseData.results());
        assertThat(result.resourceName()).isEqualTo("orders");
        assertThat(result.configs()).hasSize(1);
        assertThat(first(result.configs()).name()).isEqualTo("retention.ms");
        assertThat(first(result.configs()).value()).isEqualTo("100000");
    }

    @Test
    void describeConfigsNonTopicResourcesPassThroughUnchanged() {
        // given a broker-resource request whose name collides with a virtual topic name
        var requestData = new DescribeConfigsRequestData();
        requestData.resources().add(new DescribeConfigsRequestData.DescribeConfigsResource()
                .setResourceType(ConfigResource.Type.BROKER.id())
                .setResourceName("orders"));
        GatewayContext requestContext = freshContext();

        // when
        interceptor.onRequest(requestContext, request(DESCRIBE_CONFIGS, requestData));

        // then the non-topic resource is untouched and nothing is recorded for the response
        assertThat(first(requestData.resources()).resourceName()).isEqualTo("orders");
        assertThat(VirtualTopicState.from(requestContext).physicalToVirtual()).isEmpty();

        // given a broker response for that resource
        var responseData = new DescribeConfigsResponseData();
        responseData.results().add(new DescribeConfigsResponseData.DescribeConfigsResult()
                .setResourceType(ConfigResource.Type.BROKER.id())
                .setResourceName("orders"));

        // when
        interceptor.onResponse(requestContext, response(DESCRIBE_CONFIGS, responseData));

        // then the name passes through unchanged
        assertThat(first(responseData.results()).resourceName()).isEqualTo("orders");
    }

    @Test
    void rewritesCreateAclsTopicResourceNamesInRequest() {
        // given a create-acls request with a topic and a group resource, both named "orders"
        var requestData = new CreateAclsRequestData();
        requestData.creations().add(new CreateAclsRequestData.AclCreation()
                .setResourceType(ResourceType.TOPIC.code())
                .setResourceName("orders")
                .setResourcePatternType(PatternType.LITERAL.code())
                .setPrincipal("User:alice")
                .setHost("*")
                .setOperation(AclOperation.READ.code())
                .setPermissionType(AclPermissionType.ALLOW.code()));
        requestData.creations().add(new CreateAclsRequestData.AclCreation()
                .setResourceType(ResourceType.GROUP.code())
                .setResourceName("orders")
                .setResourcePatternType(PatternType.PREFIXED.code())
                .setPrincipal("User:bob")
                .setHost("host-1")
                .setOperation(AclOperation.WRITE.code())
                .setPermissionType(AclPermissionType.DENY.code()));

        // when
        interceptor.onRequest(context, request(CREATE_ACLS, requestData));

        // then only the topic resource is rewritten; every other field is preserved
        assertThat(requestData.creations()).extracting(
                        "resourceType", "resourceName", "resourcePatternType",
                        "principal", "host", "operation", "permissionType")
                .containsExactly(
                        tuple(ResourceType.TOPIC.code(), "orders-v2", PatternType.LITERAL.code(),
                                "User:alice", "*", AclOperation.READ.code(), AclPermissionType.ALLOW.code()),
                        tuple(ResourceType.GROUP.code(), "orders", PatternType.PREFIXED.code(),
                                "User:bob", "host-1", AclOperation.WRITE.code(), AclPermissionType.DENY.code()));
    }

    @Test
    void createAclsResponseStatusesPassThroughUnchanged() {
        // given a create-acls request for the virtual topic
        var requestData = new CreateAclsRequestData();
        requestData.creations().add(new CreateAclsRequestData.AclCreation()
                .setResourceType(ResourceType.TOPIC.code())
                .setResourceName("orders"));
        GatewayContext requestContext = freshContext();
        interceptor.onRequest(requestContext, request(CREATE_ACLS, requestData));

        // when the broker responds with per-entry statuses
        var responseData = new CreateAclsResponseData().setThrottleTimeMs(7);
        responseData.results().add(new CreateAclsResponseData.AclCreationResult()
                .setErrorCode((short) 0)
                .setErrorMessage(null));

        interceptor.onResponse(requestContext, response(CREATE_ACLS, responseData));

        // then the statuses pass through untouched
        assertThat(responseData.throttleTimeMs()).isEqualTo(7);
        assertThat(responseData.results()).hasSize(1);
        assertThat(first(responseData.results()).errorCode()).isEqualTo((short) 0);
        assertThat(first(responseData.results()).errorMessage()).isNull();
    }

    @Test
    void rewritesDeleteAclsTopicFilterNamesInRequest() {
        // given delete-acls filters for a topic, a group, and an unnamed topic filter
        var requestData = new DeleteAclsRequestData();
        requestData.filters().add(new DeleteAclsRequestData.DeleteAclsFilter()
                .setResourceTypeFilter(ResourceType.TOPIC.code())
                .setResourceNameFilter("orders")
                .setPatternTypeFilter(PatternType.LITERAL.code()));
        requestData.filters().add(new DeleteAclsRequestData.DeleteAclsFilter()
                .setResourceTypeFilter(ResourceType.GROUP.code())
                .setResourceNameFilter("orders")
                .setPatternTypeFilter(PatternType.LITERAL.code()));
        requestData.filters().add(new DeleteAclsRequestData.DeleteAclsFilter()
                .setResourceTypeFilter(ResourceType.TOPIC.code())
                .setResourceNameFilter(null)
                .setPatternTypeFilter(PatternType.ANY.code()));

        // when
        interceptor.onRequest(context, request(DELETE_ACLS, requestData));

        // then only the named topic filter is rewritten
        assertThat(requestData.filters()).extracting(
                        "resourceTypeFilter", "resourceNameFilter", "patternTypeFilter")
                .containsExactly(
                        tuple(ResourceType.TOPIC.code(), "orders-v2", PatternType.LITERAL.code()),
                        tuple(ResourceType.GROUP.code(), "orders", PatternType.LITERAL.code()),
                        tuple(ResourceType.TOPIC.code(), null, PatternType.ANY.code()));
    }

    @Test
    void rewritesDeleteAclsMatchingAclTopicNamesInResponse() {
        // given a delete-acls request for the virtual topic
        var requestData = new DeleteAclsRequestData();
        requestData.filters().add(new DeleteAclsRequestData.DeleteAclsFilter()
                .setResourceTypeFilter(ResourceType.TOPIC.code())
                .setResourceNameFilter("orders"));
        GatewayContext requestContext = freshContext();
        interceptor.onRequest(requestContext, request(DELETE_ACLS, requestData));

        // when the broker reports the matching acls under the physical name
        var responseData = new DeleteAclsResponseData();
        responseData.filterResults().add(new DeleteAclsResponseData.DeleteAclsFilterResult()
                .setErrorCode((short) 0)
                .setMatchingAcls(List.of(new DeleteAclsResponseData.DeleteAclsMatchingAcl()
                                .setResourceType(ResourceType.TOPIC.code())
                                .setResourceName("orders-v2")
                                .setPatternType(PatternType.LITERAL.code())
                                .setPrincipal("User:alice")
                                .setHost("*")
                                .setOperation(AclOperation.READ.code())
                                .setPermissionType(AclPermissionType.ALLOW.code()),
                        new DeleteAclsResponseData.DeleteAclsMatchingAcl()
                                .setResourceType(ResourceType.GROUP.code())
                                .setResourceName("orders-v2")
                                .setPatternType(PatternType.LITERAL.code())
                                .setPrincipal("User:bob")
                                .setHost("*")
                                .setOperation(AclOperation.READ.code())
                                .setPermissionType(AclPermissionType.ALLOW.code()))));

        interceptor.onResponse(requestContext, response(DELETE_ACLS, responseData));

        // then only the topic acl is renamed back; every other field is preserved
        DeleteAclsResponseData.DeleteAclsMatchingAcl topicAcl =
                first(first(responseData.filterResults()).matchingAcls());
        assertThat(topicAcl.resourceName()).isEqualTo("orders");
        assertThat(topicAcl.principal()).isEqualTo("User:alice");
        assertThat(topicAcl.host()).isEqualTo("*");
        assertThat(topicAcl.operation()).isEqualTo(AclOperation.READ.code());
        assertThat(topicAcl.permissionType()).isEqualTo(AclPermissionType.ALLOW.code());
        assertThat(topicAcl.patternType()).isEqualTo(PatternType.LITERAL.code());

        DeleteAclsResponseData.DeleteAclsMatchingAcl groupAcl =
                first(responseData.filterResults()).matchingAcls().get(1);
        assertThat(groupAcl.resourceName()).isEqualTo("orders-v2");
    }

    @Test
    void rewritesDescribeAclsTopicFilterNameInRequest() {
        // given a describe-acls request filtering the virtual topic by name
        var requestData = new DescribeAclsRequestData()
                .setResourceTypeFilter(ResourceType.TOPIC.code())
                .setResourceNameFilter("orders")
                .setPatternTypeFilter(PatternType.LITERAL.code())
                .setPrincipalFilter(null)
                .setHostFilter(null);

        // when
        interceptor.onRequest(context, request(DESCRIBE_ACLS, requestData));

        // then the filter names the physical topic and all other fields are preserved
        assertThat(requestData.resourceTypeFilter()).isEqualTo(ResourceType.TOPIC.code());
        assertThat(requestData.resourceNameFilter()).isEqualTo("orders-v2");
        assertThat(requestData.patternTypeFilter()).isEqualTo(PatternType.LITERAL.code());
        assertThat(requestData.principalFilter()).isNull();
        assertThat(requestData.hostFilter()).isNull();
    }

    @Test
    void describeAclsNonTopicFiltersPassThroughUnchanged() {
        // given a describe-acls request filtering groups by a virtual topic-like name
        var requestData = new DescribeAclsRequestData()
                .setResourceTypeFilter(ResourceType.GROUP.code())
                .setResourceNameFilter("orders");

        GatewayContext requestContext = freshContext();

        // when
        interceptor.onRequest(requestContext, request(DESCRIBE_ACLS, requestData));

        // then the filter is untouched and nothing is recorded for the response
        assertThat(requestData.resourceNameFilter()).isEqualTo("orders");
        assertThat(VirtualTopicState.from(requestContext).physicalToVirtual()).isEmpty();
    }

    @Test
    void rewritesDescribeAclsResourceTopicNamesInResponse() {
        // given a describe-acls request for the virtual topic
        var requestData = new DescribeAclsRequestData()
                .setResourceTypeFilter(ResourceType.TOPIC.code())
                .setResourceNameFilter("orders");
        GatewayContext requestContext = freshContext();
        interceptor.onRequest(requestContext, request(DESCRIBE_ACLS, requestData));

        // when the broker describes the acls under the physical topic name
        var responseData = new DescribeAclsResponseData();
        responseData.resources().add(new DescribeAclsResponseData.DescribeAclsResource()
                .setResourceType(ResourceType.TOPIC.code())
                .setResourceName("orders-v2")
                .setPatternType(PatternType.LITERAL.code())
                .setAcls(List.of(new DescribeAclsResponseData.AclDescription()
                        .setPrincipal("User:alice")
                        .setHost("*")
                        .setOperation(AclOperation.READ.code())
                        .setPermissionType(AclPermissionType.ALLOW.code()))));
        responseData.resources().add(new DescribeAclsResponseData.DescribeAclsResource()
                .setResourceType(ResourceType.GROUP.code())
                .setResourceName("orders-v2")
                .setPatternType(PatternType.LITERAL.code()));

        interceptor.onResponse(requestContext, response(DESCRIBE_ACLS, responseData));

        // then only the topic resource is renamed back; its acl descriptions are preserved
        DescribeAclsResponseData.DescribeAclsResource topicResource = first(responseData.resources());
        assertThat(topicResource.resourceName()).isEqualTo("orders");
        assertThat(topicResource.patternType()).isEqualTo(PatternType.LITERAL.code());
        assertThat(topicResource.acls()).hasSize(1);
        assertThat(first(topicResource.acls()).principal()).isEqualTo("User:alice");
        assertThat(first(topicResource.acls()).operation()).isEqualTo(AclOperation.READ.code());

        DescribeAclsResponseData.DescribeAclsResource groupResource = responseData.resources().get(1);
        assertThat(groupResource.resourceName()).isEqualTo("orders-v2");
    }

    @Test
    void rewritesDescribeTransactionsResponseTopicsToVirtualNames() {
        // given a describe-transactions request carrying only transactional ids
        var requestData = new DescribeTransactionsRequestData()
                .setTransactionalIds(List.of("txn-1"));
        interceptor.onRequest(context, request(DESCRIBE_TRANSACTIONS, requestData));
        assertThat(requestData.transactionalIds()).containsExactly("txn-1");

        // given a broker response touching the physical topic and an unmapped topic
        var responseData = new DescribeTransactionsResponseData();
        DescribeTransactionsResponseData.TransactionState state =
                new DescribeTransactionsResponseData.TransactionState()
                        .setTransactionalId("txn-1")
                        .setTransactionState("Ongoing")
                        .setTransactionTimeoutMs(60000)
                        .setTransactionStartTimeMs(123L)
                        .setProducerId(42L)
                        .setProducerEpoch((short) 7);
        state.topics().add(new DescribeTransactionsResponseData.TopicData()
                .setTopic("orders-v2")
                .setPartitions(List.of(0)));
        state.topics().add(new DescribeTransactionsResponseData.TopicData()
                .setTopic("unmapped-topic")
                .setPartitions(List.of(1, 2)));
        responseData.transactionStates().add(state);

        // when the response is rewritten without any per-request mapping
        interceptor.onResponse(freshContext(), response(DESCRIBE_TRANSACTIONS, responseData));

        // then virtual topics are renamed via the map and unmapped topics pass through
        assertThat(state.topics()).extracting("topic").containsExactly("orders", "unmapped-topic");
        assertThat(first(state.topics()).partitions()).containsExactly(0);
        assertThat(state.transactionalId()).isEqualTo("txn-1");
        assertThat(state.transactionState()).isEqualTo("Ongoing");
        assertThat(state.producerId()).isEqualTo(42L);
        assertThat(state.producerEpoch()).isEqualTo((short) 7);
    }

    @Test
    void rewritesAddPartitionsToTxnTopicNamesInRequestAndResponse() {
        // given an add-partitions-to-txn request for the virtual topic and an unmapped topic
        var requestData = new AddPartitionsToTxnRequestData()
                .setV3AndBelowTransactionalId("txn-1")
                .setV3AndBelowProducerId(42L)
                .setV3AndBelowProducerEpoch((short) 7);
        requestData.v3AndBelowTopics().add(new AddPartitionsToTxnRequestData.AddPartitionsToTxnTopic()
                .setName("orders")
                .setPartitions(List.of(0)));
        requestData.v3AndBelowTopics().add(new AddPartitionsToTxnRequestData.AddPartitionsToTxnTopic()
                .setName("unmapped-topic")
                .setPartitions(List.of(1)));

        // when
        interceptor.onRequest(context, request(ADD_PARTITIONS_TO_TXN, requestData));

        // then the topics are rewritten to the physical names, transactional metadata preserved
        assertThat(requestData.v3AndBelowTopics()).extracting("name")
                .containsExactly("orders-v2", "unmapped-topic");
        assertThat(first(requestData.v3AndBelowTopics()).partitions()).containsExactly(0);
        assertThat(requestData.v3AndBelowTransactionalId()).isEqualTo("txn-1");
        assertThat(requestData.v3AndBelowProducerId()).isEqualTo(42L);
        assertThat(requestData.v3AndBelowProducerEpoch()).isEqualTo((short) 7);

        // given a coordinator response naming the physical topic
        var responseData = new AddPartitionsToTxnResponseData();
        AddPartitionsToTxnResponseData.AddPartitionsToTxnTopicResult topicResult =
                new AddPartitionsToTxnResponseData.AddPartitionsToTxnTopicResult()
                        .setName("orders-v2");
        topicResult.resultsByPartition().add(
                new AddPartitionsToTxnResponseData.AddPartitionsToTxnPartitionResult()
                        .setPartitionIndex(0)
                        .setPartitionErrorCode((short) 0));
        responseData.resultsByTopicV3AndBelow().add(topicResult);

        // when
        interceptor.onResponse(context, response(ADD_PARTITIONS_TO_TXN, responseData));

        // then the result carries the virtual name and its partition results back to the client
        AddPartitionsToTxnResponseData.AddPartitionsToTxnTopicResult result =
                first(responseData.resultsByTopicV3AndBelow());
        assertThat(result.name()).isEqualTo("orders");
        assertThat(result.resultsByPartition()).hasSize(1);
        assertThat(first(result.resultsByPartition()).partitionIndex()).isZero();
        assertThat(first(result.resultsByPartition()).partitionErrorCode()).isZero();
    }

    @Test
    void rewritesOffsetDeleteTopicNamesInRequestAndResponse() {
        // given an offset-delete request for the virtual topic
        var requestData = new OffsetDeleteRequestData().setGroupId("group-1");
        requestData.topics().add(new OffsetDeleteRequestData.OffsetDeleteRequestTopic()
                .setName("orders")
                .setPartitions(List.of(new OffsetDeleteRequestData.OffsetDeleteRequestPartition()
                        .setPartitionIndex(0))));

        // when
        interceptor.onRequest(context, request(OFFSET_DELETE, requestData));

        // then the topic is rewritten to the physical name, group id preserved
        assertThat(first(requestData.topics()).name()).isEqualTo("orders-v2");
        assertThat(first(requestData.topics()).partitions()).hasSize(1);
        assertThat(first(first(requestData.topics()).partitions()).partitionIndex()).isZero();
        assertThat(requestData.groupId()).isEqualTo("group-1");

        // given a broker response naming the physical topic
        var responseData = new OffsetDeleteResponseData();
        OffsetDeleteResponseData.OffsetDeleteResponseTopic respTopic =
                new OffsetDeleteResponseData.OffsetDeleteResponseTopic().setName("orders-v2");
        respTopic.partitions().add(new OffsetDeleteResponseData.OffsetDeleteResponsePartition()
                .setPartitionIndex(0)
                .setErrorCode(Errors.NONE.code()));
        responseData.topics().add(respTopic);

        // when
        interceptor.onResponse(context, response(OFFSET_DELETE, responseData));

        // then the result carries the virtual name and its partition statuses back to the client
        OffsetDeleteResponseData.OffsetDeleteResponseTopic result = first(responseData.topics());
        assertThat(result.name()).isEqualTo("orders");
        assertThat(result.partitions()).hasSize(1);
        assertThat(first(result.partitions()).partitionIndex()).isZero();
        assertThat(first(result.partitions()).errorCode()).isEqualTo(Errors.NONE.code());
    }

    @Test
    void rewritesAlterConfigsTopicResourcesInRequestAndResponse() {
        // given an alter-configs request for the virtual topic plus a broker resource
        var requestData = new AlterConfigsRequestData();
        AlterConfigsRequestData.AlterConfigsResource topicResource =
                new AlterConfigsRequestData.AlterConfigsResource()
                        .setResourceType(ConfigResource.Type.TOPIC.id())
                        .setResourceName("orders");
        topicResource.configs().add(new AlterConfigsRequestData.AlterableConfig()
                .setName("retention.ms")
                .setValue("60000"));
        requestData.resources().add(topicResource);
        AlterConfigsRequestData.AlterConfigsResource brokerResource =
                new AlterConfigsRequestData.AlterConfigsResource()
                        .setResourceType(ConfigResource.Type.BROKER.id())
                        .setResourceName("orders");
        brokerResource.configs().add(new AlterConfigsRequestData.AlterableConfig()
                .setName("log.retention.hours")
                .setValue("1"));
        requestData.resources().add(brokerResource);

        // when
        interceptor.onRequest(context, request(ALTER_CONFIGS, requestData));

        // then only the topic resource is rewritten, config entries preserved
        assertThat(topicResource.resourceName()).isEqualTo("orders-v2");
        assertThat(brokerResource.resourceName()).isEqualTo("orders");
        assertThat(first(topicResource.configs()).name()).isEqualTo("retention.ms");
        assertThat(first(topicResource.configs()).value()).isEqualTo("60000");

        // given a broker response naming both resources
        var responseData = new AlterConfigsResponseData();
        responseData.responses().add(new AlterConfigsResponseData.AlterConfigsResourceResponse()
                .setResourceType(ConfigResource.Type.TOPIC.id())
                .setResourceName("orders-v2")
                .setErrorCode(Errors.NONE.code()));
        responseData.responses().add(new AlterConfigsResponseData.AlterConfigsResourceResponse()
                .setResourceType(ConfigResource.Type.BROKER.id())
                .setResourceName("orders")
                .setErrorCode(Errors.NONE.code()));

        // when
        interceptor.onResponse(context, response(ALTER_CONFIGS, responseData));

        // then only the topic result carries the virtual name back to the client
        assertThat(responseData.responses()).extracting("resourceName")
                .containsExactly("orders", "orders");
    }

    @Test
    void rewritesIncrementalAlterConfigsTopicResourcesInRequestAndResponse() {
        // given an incremental-alter-configs request setting a config on the virtual topic
        var requestData = new IncrementalAlterConfigsRequestData();
        IncrementalAlterConfigsRequestData.AlterConfigsResource resource =
                new IncrementalAlterConfigsRequestData.AlterConfigsResource()
                        .setResourceType(ConfigResource.Type.TOPIC.id())
                        .setResourceName("orders");
        resource.configs().add(new IncrementalAlterConfigsRequestData.AlterableConfig()
                .setName("retention.ms")
                .setValue("120000")
                .setConfigOperation((byte) 0));
        requestData.resources().add(resource);

        // when
        interceptor.onRequest(context, request(INCREMENTAL_ALTER_CONFIGS, requestData));

        // then the resource is rewritten to the physical name, config entry preserved
        assertThat(resource.resourceName()).isEqualTo("orders-v2");
        assertThat(first(resource.configs()).name()).isEqualTo("retention.ms");
        assertThat(first(resource.configs()).value()).isEqualTo("120000");
        assertThat(first(resource.configs()).configOperation()).isEqualTo((byte) 0);

        // given a broker response naming the physical topic
        var responseData = new IncrementalAlterConfigsResponseData();
        responseData.responses().add(new IncrementalAlterConfigsResponseData.AlterConfigsResourceResponse()
                .setResourceType(ConfigResource.Type.TOPIC.id())
                .setResourceName("orders-v2")
                .setErrorCode(Errors.NONE.code()));

        // when
        interceptor.onResponse(context, response(INCREMENTAL_ALTER_CONFIGS, responseData));

        // then the result carries the virtual name back to the client
        assertThat(first(responseData.responses()).resourceName()).isEqualTo("orders");
        assertThat(first(responseData.responses()).errorCode()).isEqualTo(Errors.NONE.code());
    }

    @Test
    void rewritesDeleteRecordsTopicNamesInRequestAndResponse() {
        // given a delete-records request truncating the virtual topic's partition
        var requestData = new DeleteRecordsRequestData().setTimeoutMs(5000);
        requestData.topics().add(new DeleteRecordsRequestData.DeleteRecordsTopic()
                .setName("orders")
                .setPartitions(List.of(new DeleteRecordsRequestData.DeleteRecordsPartition()
                        .setPartitionIndex(0)
                        .setOffset(3L))));

        // when
        interceptor.onRequest(context, request(DELETE_RECORDS, requestData));

        // then the topic is rewritten to the physical name, timeout and offsets preserved
        assertThat(first(requestData.topics()).name()).isEqualTo("orders-v2");
        assertThat(first(first(requestData.topics()).partitions()).partitionIndex()).isZero();
        assertThat(first(first(requestData.topics()).partitions()).offset()).isEqualTo(3L);
        assertThat(requestData.timeoutMs()).isEqualTo(5000);

        // given a broker response naming the physical topic
        var responseData = new DeleteRecordsResponseData();
        DeleteRecordsResponseData.DeleteRecordsTopicResult topicResult =
                new DeleteRecordsResponseData.DeleteRecordsTopicResult().setName("orders-v2");
        topicResult.partitions().add(new DeleteRecordsResponseData.DeleteRecordsPartitionResult()
                .setPartitionIndex(0)
                .setLowWatermark(3L));
        responseData.topics().add(topicResult);

        // when
        interceptor.onResponse(context, response(DELETE_RECORDS, responseData));

        // then the result carries the virtual name and its low watermark back to the client
        DeleteRecordsResponseData.DeleteRecordsTopicResult result = first(responseData.topics());
        assertThat(result.name()).isEqualTo("orders");
        assertThat(first(result.partitions()).partitionIndex()).isZero();
        assertThat(first(result.partitions()).lowWatermark()).isEqualTo(3L);
    }

    @Test
    void rewritesFindCoordinatorEndpoint() {
        var responseData = new FindCoordinatorResponseData();
        responseData.setNodeId(0);
        responseData.coordinators().add(new FindCoordinatorResponseData.Coordinator()
                .setNodeId(0).setHost("coordinator-internal").setPort(9093));

        interceptor.onResponse(context, response(FIND_COORDINATOR, responseData));

        assertThat(responseData.host()).isEqualTo("gateway.example.com");
        assertThat(responseData.port()).isEqualTo(9092);
        assertThat(first(responseData.coordinators()).host()).isEqualTo("gateway.example.com");
        assertThat(first(responseData.coordinators()).port()).isEqualTo(9092);
    }

    @Test
    void unmappedTopicsPassThroughUnchanged() {
        var data = new MetadataResponseData();
        addTopic(data, "orders-v2");
        addTopic(data, "unmapped-topic");

        interceptor.onResponse(context, response(METADATA, data));

        // unmapped topics pass through unchanged; the virtualized one is renamed to its virtual
        // name in place, not exposed alongside its physical name
        assertThat(data.topics()).extracting("name").containsExactly("orders", "unmapped-topic");
    }

    @Test
    void exposePhysicalTopicOptInListsBothNames() {
        var data = new MetadataResponseData();
        addTopic(data, "orders-v2");
        addTopic(data, "legacy-v1");

        interceptor.onResponse(context, response(METADATA, data));

        // "orders-v2" is hidden (renamed in place) as usual; "legacy-v1" opted in via
        // exposePhysicalTopic: true, so it keeps its physical entry AND gets a virtual one added
        assertThat(data.topics()).extracting("name")
                .containsExactly("orders", "legacy-v1", "legacy");
    }

    @Test
    void idleIncrementalFetchResponseIsRenamedViaSession() {
        var sessions = new FetchSessionRegistry();
        var sessionInterceptor = new VirtualTopicInterceptor(virtualTopics, advertised, sessions);

        var create = new FetchRequestData();
        create.topics().add(new FetchRequestData.FetchTopic().setTopic("customers"));
        GatewayContext createContext = freshContext();
        sessionInterceptor.onRequest(createContext, request(FETCH, create));
        assertThat(first(create.topics()).topic()).isEqualTo("customers-v2");

        var createdResponse = new FetchResponseData().setSessionId(42);
        createdResponse.responses().add(new FetchResponseData.FetchableTopicResponse().setTopic("customers-v2"));
        sessionInterceptor.onResponse(createContext, response(FETCH, createdResponse));
        assertThat(first(createdResponse.responses()).topic()).isEqualTo("customers");
        assertThat(sessions.hasSession("test", 42)).isTrue();

        var incremental = new FetchRequestData().setSessionId(42).setSessionEpoch(1);
        sessionInterceptor.onRequest(freshContext(), request(FETCH, incremental));

        var incrementalResponse = new FetchResponseData().setSessionId(42);
        incrementalResponse.responses().add(new FetchResponseData.FetchableTopicResponse().setTopic("customers-v2"));
        sessionInterceptor.onResponse(freshContext(), response(FETCH, incrementalResponse));
        assertThat(first(incrementalResponse.responses()).topic()).isEqualTo("customers");
    }

    @Test
    void closingFetchSessionDropsRegistryEntry() {
        var sessions = new FetchSessionRegistry();
        var sessionInterceptor = new VirtualTopicInterceptor(virtualTopics, advertised, sessions);
        bindSession(sessionInterceptor, sessions);

        var close = new FetchRequestData().setSessionId(42).setSessionEpoch(-1);
        sessionInterceptor.onRequest(freshContext(), request(FETCH, close));

        assertThat(sessions.hasSession("test", 42)).isFalse();
    }

    @Test
    void unknownSessionResponseDropsRegistryEntry() {
        var sessions = new FetchSessionRegistry();
        var sessionInterceptor = new VirtualTopicInterceptor(virtualTopics, advertised, sessions);
        bindSession(sessionInterceptor, sessions);

        var error = new FetchResponseData().setSessionId(42)
                .setErrorCode(Errors.FETCH_SESSION_ID_NOT_FOUND.code());
        sessionInterceptor.onResponse(freshContext(), response(FETCH, error));

        assertThat(sessions.hasSession("test", 42)).isFalse();
    }

    @Test
    void forgottenTopicsAreRenamedAndDroppedFromSession() {
        var sessions = new FetchSessionRegistry();
        var sessionInterceptor = new VirtualTopicInterceptor(virtualTopics, advertised, sessions);
        bindSession(sessionInterceptor, sessions);

        var forget = new FetchRequestData().setSessionId(42).setSessionEpoch(1);
        forget.forgottenTopicsData().add(new FetchRequestData.ForgottenTopic().setTopic("customers"));
        sessionInterceptor.onRequest(freshContext(), request(FETCH, forget));

        assertThat(first(forget.forgottenTopicsData()).topic()).isEqualTo("customers-v2");
        assertThat(sessions.virtualFor("test", 42, "customers-v2")).isNull();
    }

    private static void bindSession(
            VirtualTopicInterceptor sessionInterceptor,
            FetchSessionRegistry sessions
    ) {
        var create = new FetchRequestData();
        create.topics().add(new FetchRequestData.FetchTopic().setTopic("customers"));
        GatewayContext createContext = freshContext();
        sessionInterceptor.onRequest(createContext, request(FETCH, create));
        sessionInterceptor.onResponse(createContext, response(FETCH, new FetchResponseData().setSessionId(42)));
        assertThat(sessions.hasSession("test", 42)).isTrue();
    }

    @Test
    void filtersFetchResponseRecordsUsingCelExpression() {
        // given a virtual topic backed by a CEL filter on tenant=acme
        var celTopics = new VirtualTopicManager(Map.of(
                "orders", new VirtualTopicConfig("orders-v2",
                        new CelFilterConfig("headers.tenant == \"acme\""))));
        var celInterceptor = new VirtualTopicInterceptor(celTopics, advertised);

        var create = new FetchRequestData();
        create.topics().add(new FetchRequestData.FetchTopic().setTopic("orders"));
        GatewayContext ctx = freshContext();
        celInterceptor.onRequest(ctx, request(FETCH, create));
        assertThat(first(create.topics()).topic()).isEqualTo("orders-v2");

        // when a fetch response carries records with mixed tenant headers
        var matching = new SimpleRecord(
                1000L, "k1".getBytes(StandardCharsets.UTF_8), "v1".getBytes(StandardCharsets.UTF_8),
                new Header[]{new RecordHeader("tenant", "acme".getBytes(StandardCharsets.UTF_8))});
        var nonMatching = new SimpleRecord(
                2000L, "k2".getBytes(StandardCharsets.UTF_8), "v2".getBytes(StandardCharsets.UTF_8),
                new Header[]{new RecordHeader("tenant", "other".getBytes(StandardCharsets.UTF_8))});
        var response = new FetchResponseData();
        response.responses().add(new FetchResponseData.FetchableTopicResponse()
                .setTopic("orders-v2")
                .setPartitions(List.of(new FetchResponseData.PartitionData()
                        .setPartitionIndex(0)
                        .setRecords(MemoryRecords.withRecords(Compression.NONE, matching, nonMatching)))));
        celInterceptor.onResponse(ctx, response(FETCH, response));

        // then only the matching record survives
        var filtered = (MemoryRecords) first(response.responses()).partitions().get(0).records();
        List<Record> survivors = new ArrayList<>();
        filtered.records().forEach(survivors::add);
        assertThat(survivors).hasSize(1);
        assertThat(StandardCharsets.UTF_8.decode(survivors.get(0).key()).toString()).isEqualTo("k1");
    }

    @Test
    void filtersFetchResponseRecordsByJsonContent() {
        // given a virtual topic filtering on JSON content
        var virtualTopicMan = new VirtualTopicManager(
                Map.of(
                        "paid-orders", new VirtualTopicConfig(
                                "orders",
                                new CelFilterConfig("value.status == \"PAID\""), false, new JsonFormatConfig()
                        )
                )
        );
        var interceptor = new VirtualTopicInterceptor(virtualTopicMan, advertised);

        var requestData = new FetchRequestData();
        requestData.topics().add(new FetchRequestData.FetchTopic().setTopic("paid-orders"));
        var gatewayContext = freshContext();
        interceptor.onRequest(gatewayContext, request(FETCH, requestData));
        assertThat(first(requestData.topics()).topic()).isEqualTo("orders");

        var matching = new SimpleRecord(1000L, "k1".getBytes(StandardCharsets.UTF_8),
                "{\"status\":\"PAID\"}".getBytes(StandardCharsets.UTF_8));
        var nonMatching = new SimpleRecord(2000L, "k2".getBytes(StandardCharsets.UTF_8),
                "{\"status\":\"OPEN\"}".getBytes(StandardCharsets.UTF_8));
        var records = MemoryRecords.withRecords(Compression.NONE, matching, nonMatching);

        var responseData = new FetchResponseData();
        responseData.responses().add(new FetchResponseData.FetchableTopicResponse()
                .setTopic("orders")
                .setPartitions(List.of(new FetchResponseData.PartitionData()
                        .setPartitionIndex(0)
                        .setRecords(records))));

        interceptor.onResponse(gatewayContext, response(FETCH, responseData));

        // then the topic is renamed back AND its records are filtered by JSON content
        assertThat(first(responseData.responses()).topic()).isEqualTo("paid-orders");
        var filtered = (MemoryRecords) first(responseData.responses()).partitions().getFirst().records();
        List<Record> survivors = new ArrayList<>();
        filtered.records().forEach(survivors::add);
        assertThat(survivors).hasSize(1);
        assertThat(StandardCharsets.UTF_8.decode(survivors.getFirst().value()).toString())
                .withFailMessage(() -> "Non-matching JSON record was not filtered from the fetch response")
                .isEqualTo("{\"status\":\"PAID\"}");
    }

    @Test
    void nullBodyIsIgnored() {
        interceptor.onRequest(context, request(METADATA, null));
        interceptor.onResponse(context, response(METADATA, null));
    }

    private static <T> T first(Iterable<T> iterable) {
        return iterable.iterator().next();
    }

    private static Object invokeNoArg(
            Object target,
            String methodName
    ) {
        try {
            return target.getClass().getMethod(methodName).invoke(target);
        } catch (ReflectiveOperationException e) {
            fail("Expected method " + methodName + " on " + target.getClass().getName(), e);
            return null;
        }
    }

    private static GatewayContext freshContext() {
        return new GatewayContext("test", System.nanoTime());
    }
}

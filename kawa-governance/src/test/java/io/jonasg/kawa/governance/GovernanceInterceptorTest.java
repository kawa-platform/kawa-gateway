package io.jonasg.kawa.governance;

import io.jonasg.kawa.config.GovernanceConfig;
import io.jonasg.kawa.config.GovernanceRuleConfig;
import io.jonasg.kawa.config.GovernanceRuleConfig.Expression;
import io.jonasg.kawa.config.GovernanceRuleConfig.Operation;
import io.jonasg.kawa.config.GovernanceRuleConfig.Selector;
import io.jonasg.kawa.config.GovernanceRuleConfig.TopicScope;
import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.core.Request;
import io.jonasg.kawa.core.Response;
import io.jonasg.kawa.governance.TopicDescriber.TopicState;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.message.AlterConfigsRequestData;
import org.apache.kafka.common.message.AlterConfigsResponseData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.message.CreatePartitionsRequestData;
import org.apache.kafka.common.message.CreatePartitionsResponseData;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.DeleteTopicsRequestData;
import org.apache.kafka.common.message.DeleteTopicsResponseData;
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData;
import org.apache.kafka.common.message.IncrementalAlterConfigsResponseData;
import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.message.InitProducerIdResponseData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData;
import org.apache.kafka.common.message.TxnOffsetCommitResponseData;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class GovernanceInterceptorTest {

    private static final GovernanceRuleConfig NAMING = new GovernanceRuleConfig(
            "naming", "topic names start with app.", null, Selector.topic(), Expression.cel("topic.name.startsWith('app.')"));

    private static final GovernanceRuleConfig COMPACTED_ON_ALTER = new GovernanceRuleConfig(
            "compacted", "cleanup.policy must be compact", null,
            new Selector(ResourceType.TOPIC, null, TopicScope.BOTH, Set.of(Operation.CREATE, Operation.ALTER)),
            Expression.cel("!('cleanup.policy' in topic.configs) || topic.configs['cleanup.policy'] == 'compact'"));

    private static final GovernanceRuleConfig GROUP_NAMING = new GovernanceRuleConfig(
            "group-naming", "groups start with app-", null, new Selector(ResourceType.GROUP, null),
            Expression.cel("group.id.startsWith('app-')"));

    private static final GovernanceRuleConfig TRANSACTION_NAMING = new GovernanceRuleConfig(
            "txn-naming", "transactional ids start with app-", null, new Selector(ResourceType.TRANSACTIONAL_ID, null),
            Expression.cel("transaction.id.startsWith('app-')"));

    private static final GovernanceRuleConfig TIERS_ON_ALTER = new GovernanceRuleConfig(
            "tiers", "partitions must be 1 or 3", null,
            new Selector(ResourceType.TOPIC, null, TopicScope.BOTH, Set.of(Operation.CREATE, Operation.ALTER)),
            Expression.cel("topic.partitions in [1, 3]"));

    private static final GovernanceRuleConfig KEEP_COMPACTED = new GovernanceRuleConfig(
            "keep-compacted", "compacted topics are kept", null,
            new Selector(ResourceType.TOPIC, null, TopicScope.PHYSICAL, Set.of(Operation.DELETE)),
            Expression.cel("!('cleanup.policy' in topic.configs) || topic.configs['cleanup.policy'] != 'compact'"));

    private final GatewayContext context = new GatewayContext(null, 0L, "User:alice");

    private final GovernanceInterceptor ids = interceptor(Map.of("group-naming", GROUP_NAMING, "txn-naming", TRANSACTION_NAMING));

    @Test
    void stripsRefusedTopicsFromCreateTopicsAndAnswersThemWithPolicyViolation() {
        // given
        var interceptor = interceptor(Map.of("naming", NAMING));
        var data = new CreateTopicsRequestData();
        data.topics().add(new CreateTopicsRequestData.CreatableTopic().setName("app.orders").setNumPartitions(3).setReplicationFactor((short) 3));
        data.topics().add(new CreateTopicsRequestData.CreatableTopic().setName("orders").setNumPartitions(3).setReplicationFactor((short) 3));

        // when
        interceptor.onRequest(context, request(ApiKeys.CREATE_TOPICS, data));
        var response = new CreateTopicsResponseData();
        response.topics().add(new CreateTopicsResponseData.CreatableTopicResult().setName("app.orders"));
        interceptor.onResponse(context, response(ApiKeys.CREATE_TOPICS, response));

        // then
        assertThat(data.topics()).extracting(CreateTopicsRequestData.CreatableTopic::name)
                .withFailMessage(() -> "Refused topic 'orders' was forwarded to the broker")
                .containsExactly("app.orders");
        var refused = response.topics().find("orders");
        assertThat(refused.errorCode()).isEqualTo(Errors.POLICY_VIOLATION.code());
        assertThat(refused.errorMessage()).isEqualTo("[naming] topic names start with app.");
        assertThat(context.isShortCircuited()).isFalse();
    }

    @Test
    void alterRunsOnlyRulesThatRunOnAlter() {
        // given
        var interceptor = interceptor(Map.of("naming", NAMING, "compacted", COMPACTED_ON_ALTER), Map.of("orders", topic(Map.of())));
        var data = new AlterConfigsRequestData();
        data.resources().add(alterConfigs("orders", Map.of("cleanup.policy", "delete")));

        // when
        var response = new AlterConfigsResponseData();
        intercept(interceptor, ApiKeys.ALTER_CONFIGS, data, response);

        // then
        assertThat(data.resources()).isEmpty();
        assertThat(response.responses()).singleElement()
                .satisfies(r -> {
                    assertThat(r.resourceName()).isEqualTo("orders");
                    assertThat(r.errorMessage())
                            .withFailMessage(() -> "Create-only rule 'naming' ran on alter: " + r.errorMessage())
                            .isEqualTo("[compacted] cleanup.policy must be compact");
                });
    }

    @Test
    void alterConfigsReplacesEveryOverride() {
        // given - the topic is not compacted today, and AlterConfigs drops that override
        var interceptor = interceptor(Map.of("compacted", COMPACTED_ON_ALTER),
                Map.of("orders", topic(Map.of("cleanup.policy", "delete"))));
        var data = new AlterConfigsRequestData();
        data.resources().add(alterConfigs("orders", Map.of("retention.ms", "1000")));

        // when
        intercept(interceptor, ApiKeys.ALTER_CONFIGS, data, new AlterConfigsResponseData());

        // then
        assertThat(data.resources())
                .withFailMessage(() -> "AlterConfigs was judged with an override it removes")
                .hasSize(1);
    }

    @Test
    void incrementalAlterIsJudgedWithTheTopicsOtherConfigs() {
        // given - the topic is not compacted today; the change does not touch cleanup.policy
        var interceptor = interceptor(Map.of("compacted", COMPACTED_ON_ALTER),
                Map.of("orders", topic(Map.of("cleanup.policy", "delete"))));
        var data = new IncrementalAlterConfigsRequestData();
        data.resources().add(incrementalAlter("orders", op("segment.ms", "60000", 0)));

        // when
        var response = new IncrementalAlterConfigsResponseData();
        intercept(interceptor, ApiKeys.INCREMENTAL_ALTER_CONFIGS, data, response);

        // then
        assertThat(data.resources())
                .withFailMessage(() -> "A change was judged without the topic's current cleanup.policy")
                .isEmpty();
        assertThat(response.responses()).singleElement()
                .satisfies(r -> assertThat(r.errorMessage()).isEqualTo("[compacted] cleanup.policy must be compact"));
    }

    @Test
    void incrementalDeleteRemovesTheOverrideBeforeJudging() {
        // given
        var interceptor = interceptor(Map.of("compacted", COMPACTED_ON_ALTER),
                Map.of("orders", topic(Map.of("cleanup.policy", "delete"))));
        var data = new IncrementalAlterConfigsRequestData();
        data.resources().add(incrementalAlter("orders", op("cleanup.policy", null, 1)));

        // when
        intercept(interceptor, ApiKeys.INCREMENTAL_ALTER_CONFIGS, data, new IncrementalAlterConfigsResponseData());

        // then
        assertThat(data.resources())
                .withFailMessage(() -> "Deleting the offending override was refused")
                .hasSize(1);
    }

    @Test
    void appendAndSubtractWorkOnLists() {
        // when
        var configs = GovernanceInterceptor.applyIncremental(Map.of("cleanup.policy", "delete", "x", "a,b"), List.of(
                op("cleanup.policy", "compact", 2),
                op("cleanup.policy", "delete", 3),
                op("x", "b", 2),
                op("y", "c", 2)));

        // then
        assertThat(configs).containsExactlyInAnyOrderEntriesOf(Map.of("cleanup.policy", "compact", "x", "a,b", "y", "c"));
    }

    @Test
    void createPartitionsIsJudgedWithTheNewPartitionCount() {
        // given
        var interceptor = interceptor(Map.of("tiers", TIERS_ON_ALTER), Map.of("orders", topic(Map.of())));
        var data = new CreatePartitionsRequestData();
        data.topics().add(new CreatePartitionsRequestData.CreatePartitionsTopic().setName("orders").setCount(2));

        // when
        var response = new CreatePartitionsResponseData();
        intercept(interceptor, ApiKeys.CREATE_PARTITIONS, data, response);

        // then
        assertThat(data.topics()).isEmpty();
        assertThat(response.results()).singleElement().satisfies(r -> {
            assertThat(r.name()).isEqualTo("orders");
            assertThat(r.errorCode()).isEqualTo(Errors.POLICY_VIOLATION.code());
            assertThat(r.errorMessage()).isEqualTo("[tiers] partitions must be 1 or 3");
        });
    }

    @Test
    void judgesAChangeThroughAVirtualNameAsAChangeToItsPhysicalTopic() {
        // given - "orders-view" is a virtual topic over "orders"
        var interceptor = new GovernanceInterceptor(() -> new GovernancePolicy(new GovernanceConfig(Map.of("tiers", TIERS_ON_ALTER))),
                names -> CompletableFuture.completedFuture(names.contains("orders") ? Map.of("orders", topic(Map.of())) : Map.of()),
                name -> name.equals("orders-view") ? "orders" : name);
        var data = new CreatePartitionsRequestData();
        data.topics().add(new CreatePartitionsRequestData.CreatePartitionsTopic().setName("orders-view").setCount(2));

        // when
        var response = new CreatePartitionsResponseData();
        intercept(interceptor, ApiKeys.CREATE_PARTITIONS, data, response);

        // then
        assertThat(data.topics())
                .withFailMessage(() -> "Partitions were added through a virtual name without judging the physical topic")
                .isEmpty();
        assertThat(response.results()).singleElement().satisfies(r -> assertThat(r.name()).isEqualTo("orders-view"));
    }

    @Test
    void leavesCreatingATopicUnderAVirtualNameToTheVirtualTopicInterceptor() {
        // given
        var interceptor = new GovernanceInterceptor(() -> new GovernancePolicy(new GovernanceConfig(Map.of("naming", NAMING))),
                names -> CompletableFuture.completedFuture(Map.of()),
                name -> name.equals("orders-view") ? "orders" : name);
        var data = new CreateTopicsRequestData();
        data.topics().add(new CreateTopicsRequestData.CreatableTopic().setName("orders-view").setNumPartitions(1).setReplicationFactor((short) 1));

        // when
        interceptor.onRequest(context, request(ApiKeys.CREATE_TOPICS, data));

        // then
        assertThat(data.topics()).hasSize(1);
    }

    @Test
    void refusesDeletingAProtectedTopicAgainstItsCurrentState() {
        // given - compacted topics may not be deleted
        var interceptor = interceptor(Map.of("keep-compacted", KEEP_COMPACTED),
                Map.of("orders", topic(Map.of("cleanup.policy", "compact")), "scratch", topic(Map.of())));
        var data = new DeleteTopicsRequestData();
        data.topics().add(new DeleteTopicsRequestData.DeleteTopicState().setName("orders"));
        data.topics().add(new DeleteTopicsRequestData.DeleteTopicState().setName("scratch"));

        // when
        var response = new DeleteTopicsResponseData();
        intercept(interceptor, ApiKeys.DELETE_TOPICS, data, response);

        // then
        assertThat(data.topics()).extracting(DeleteTopicsRequestData.DeleteTopicState::name)
                .withFailMessage(() -> "Compacted 'orders' was forwarded for deletion")
                .containsExactly("scratch");
        assertThat(response.responses()).singleElement().satisfies(r -> {
            assertThat(r.name()).isEqualTo("orders");
            assertThat(r.errorCode()).isEqualTo(Errors.POLICY_VIOLATION.code());
            assertThat(r.errorMessage()).isEqualTo("[keep-compacted] compacted topics are kept");
        });
    }

    @Test
    void judgesDeletesByNameFromOlderClients() {
        // given
        var interceptor = interceptor(Map.of("keep-compacted", KEEP_COMPACTED), Map.of("orders", topic(Map.of("cleanup.policy", "compact"))));
        var data = new DeleteTopicsRequestData();
        data.topicNames().add("orders");

        // when
        intercept(interceptor, ApiKeys.DELETE_TOPICS, data, new DeleteTopicsResponseData());

        // then
        assertThat(data.topicNames()).isEmpty();
    }

    @Test
    void refusesADeleteByTopicIdWhileARuleRunsOnDelete() {
        // given
        var interceptor = interceptor(Map.of("keep-compacted", KEEP_COMPACTED), Map.of());
        var id = Uuid.randomUuid();
        var data = new DeleteTopicsRequestData();
        data.topics().add(new DeleteTopicsRequestData.DeleteTopicState().setName(null).setTopicId(id));

        // when
        var response = new DeleteTopicsResponseData();
        intercept(interceptor, ApiKeys.DELETE_TOPICS, data, response);

        // then
        assertThat(data.topics()).isEmpty();
        assertThat(response.responses()).singleElement().satisfies(r -> {
            assertThat(r.topicId()).isEqualTo(id);
            assertThat(r.errorCode()).isEqualTo(Errors.POLICY_VIOLATION.code());
        });
    }

    @Test
    void deletesAreNotJudgedWhileNoRuleRunsOnDelete() {
        // given
        var interceptor = new GovernanceInterceptor(new GovernancePolicy(new GovernanceConfig(Map.of("tiers", TIERS_ON_ALTER))),
                _ -> {
                    throw new AssertionError("the broker was asked although no rule runs on delete");
                });
        var data = new DeleteTopicsRequestData();
        data.topics().add(new DeleteTopicsRequestData.DeleteTopicState().setName(null).setTopicId(Uuid.randomUuid()));

        // when
        var prepared = interceptor.prepare(context, request(ApiKeys.DELETE_TOPICS, data));
        interceptor.onRequest(context, request(ApiKeys.DELETE_TOPICS, data));

        // then
        assertThat(prepared).isNull();
        assertThat(data.topics()).hasSize(1);
    }

    @Test
    void forwardsAChangeToATopicTheBrokerDoesNotKnow() {
        // given
        var interceptor = interceptor(Map.of("tiers", TIERS_ON_ALTER), Map.of());
        var data = new CreatePartitionsRequestData();
        data.topics().add(new CreatePartitionsRequestData.CreatePartitionsTopic().setName("ghost").setCount(2));

        // when
        intercept(interceptor, ApiKeys.CREATE_PARTITIONS, data, new CreatePartitionsResponseData());

        // then
        assertThat(data.topics())
                .withFailMessage(() -> "A change to an unknown topic was refused instead of left to the broker")
                .hasSize(1);
    }

    @Test
    void refusesAChangeWhenTheTopicCannotBeRead() {
        // given
        var interceptor = new GovernanceInterceptor(new GovernancePolicy(new GovernanceConfig(Map.of("tiers", TIERS_ON_ALTER))),
                _ -> CompletableFuture.failedFuture(new IllegalStateException("broker unreachable")));
        var data = new CreatePartitionsRequestData();
        data.topics().add(new CreatePartitionsRequestData.CreatePartitionsTopic().setName("orders").setCount(3));

        // when
        var response = new CreatePartitionsResponseData();
        intercept(interceptor, ApiKeys.CREATE_PARTITIONS, data, response);

        // then
        assertThat(data.topics()).isEmpty();
        assertThat(response.results()).singleElement().satisfies(r -> assertThat(r.errorMessage())
                .isEqualTo("governance could not read the current state of topic 'orders': broker unreachable"));
    }

    @Test
    void readsNothingWhileNoRuleRunsOnAlter() {
        // given
        var interceptor = new GovernanceInterceptor(new GovernancePolicy(new GovernanceConfig(Map.of("naming", NAMING))),
                _ -> {
                    throw new AssertionError("the broker was asked although no rule runs on alter");
                });
        var data = new CreatePartitionsRequestData();
        data.topics().add(new CreatePartitionsRequestData.CreatePartitionsTopic().setName("orders").setCount(2));

        // when
        var prepared = interceptor.prepare(context, request(ApiKeys.CREATE_PARTITIONS, data));
        interceptor.onRequest(context, request(ApiKeys.CREATE_PARTITIONS, data));

        // then
        assertThat(prepared).isNull();
        assertThat(data.topics()).hasSize(1);
    }

    @Test
    void refusesJoinGroupWithInvalidGroupId() {
        // when
        ids.onRequest(context, request(ApiKeys.JOIN_GROUP, new JoinGroupRequestData().setGroupId("payments")));

        // then
        assertThat(context.isShortCircuited())
                .withFailMessage(() -> "JoinGroup for non-compliant group 'payments' was forwarded")
                .isTrue();
        var body = (JoinGroupResponseData) context.shortCircuitResult().body();
        assertThat(body.errorCode()).isEqualTo(Errors.INVALID_GROUP_ID.code());
    }

    @Test
    void forwardsJoinGroupForACompliantGroup() {
        // when
        ids.onRequest(context, request(ApiKeys.JOIN_GROUP, new JoinGroupRequestData().setGroupId("app-payments")));

        // then
        assertThat(context.isShortCircuited()).isFalse();
    }

    @Test
    void refusesConsumerGroupHeartbeatWithTheGovernanceMessage() {
        // when
        ids.onRequest(context, request(ApiKeys.CONSUMER_GROUP_HEARTBEAT,
                new ConsumerGroupHeartbeatRequestData().setGroupId("payments").setMemberEpoch(0)));

        // then
        var body = (ConsumerGroupHeartbeatResponseData) context.shortCircuitResult().body();
        assertThat(body.errorCode()).isEqualTo(Errors.INVALID_GROUP_ID.code());
        assertThat(body.errorMessage()).isEqualTo("[group-naming] groups start with app-");
    }

    @Test
    void letsAMemberLeaveAGroupItMayNoLongerUse() {
        // when
        ids.onRequest(context, request(ApiKeys.CONSUMER_GROUP_HEARTBEAT,
                new ConsumerGroupHeartbeatRequestData().setGroupId("payments").setMemberEpoch(-1)));

        // then
        assertThat(context.isShortCircuited())
                .withFailMessage(() -> "A leave heartbeat was refused")
                .isFalse();
    }

    @Test
    void stripsARefusedOffsetCommitAndAnswersEveryPartitionWithInvalidGroupId() {
        // given
        var data = new OffsetCommitRequestData().setGroupId("payments");
        data.topics().add(new OffsetCommitRequestData.OffsetCommitRequestTopic().setName("orders").setPartitions(List.of(
                new OffsetCommitRequestData.OffsetCommitRequestPartition().setPartitionIndex(0),
                new OffsetCommitRequestData.OffsetCommitRequestPartition().setPartitionIndex(1))));

        // when
        ids.onRequest(context, request(ApiKeys.OFFSET_COMMIT, data));
        var response = new OffsetCommitResponseData();
        ids.onResponse(context, response(ApiKeys.OFFSET_COMMIT, response));

        // then
        assertThat(data.topics())
                .withFailMessage(() -> "Offsets of a refused group were forwarded: " + data.topics())
                .isEmpty();
        assertThat(context.isShortCircuited()).isFalse();
        assertThat(response.topics()).singleElement().satisfies(topic -> {
            assertThat(topic.name()).isEqualTo("orders");
            assertThat(topic.partitions()).extracting(OffsetCommitResponseData.OffsetCommitResponsePartition::partitionIndex)
                    .containsExactly(0, 1);
            assertThat(topic.partitions()).extracting(OffsetCommitResponseData.OffsetCommitResponsePartition::errorCode)
                    .containsOnly(Errors.INVALID_GROUP_ID.code());
        });
    }

    @Test
    void stripsARefusedTxnOffsetCommit() {
        // given
        var data = new TxnOffsetCommitRequestData().setGroupId("payments").setTransactionalId("app-tx");
        data.topics().add(new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic().setName("orders").setPartitions(List.of(
                new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition().setPartitionIndex(3))));

        // when
        ids.onRequest(context, request(ApiKeys.TXN_OFFSET_COMMIT, data));
        var response = new TxnOffsetCommitResponseData();
        ids.onResponse(context, response(ApiKeys.TXN_OFFSET_COMMIT, response));

        // then
        assertThat(data.topics()).isEmpty();
        assertThat(response.topics()).singleElement().satisfies(topic ->
                assertThat(topic.partitions()).singleElement().satisfies(p -> {
                    assertThat(p.partitionIndex()).isEqualTo(3);
                    assertThat(p.errorCode()).isEqualTo(Errors.INVALID_GROUP_ID.code());
                }));
    }

    @Test
    void refusesInitProducerIdForANonCompliantTransactionalId() {
        // when
        ids.onRequest(context, request(ApiKeys.INIT_PRODUCER_ID, new InitProducerIdRequestData().setTransactionalId("payments-tx")));

        // then
        var body = (InitProducerIdResponseData) context.shortCircuitResult().body();
        assertThat(body.errorCode()).isEqualTo(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code());
        assertThat(body.producerId()).isEqualTo(-1L);
    }

    @Test
    void forwardsInitProducerIdOfAnIdempotentProducer() {
        // when
        ids.onRequest(context, request(ApiKeys.INIT_PRODUCER_ID, new InitProducerIdRequestData().setTransactionalId(null)));

        // then
        assertThat(context.isShortCircuited())
                .withFailMessage(() -> "An idempotent producer without a transactional id was refused")
                .isFalse();
    }

    private static GovernanceInterceptor interceptor(Map<String, GovernanceRuleConfig> rules) {
        return interceptor(rules, Map.of());
    }

    private static GovernanceInterceptor interceptor(Map<String, GovernanceRuleConfig> rules, Map<String, TopicState> topics) {
        return new GovernanceInterceptor(new GovernancePolicy(new GovernanceConfig(rules)),
                names -> CompletableFuture.completedFuture(topics.entrySet().stream()
                        .filter(e -> names.contains(e.getKey()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))));
    }

    private static TopicState topic(Map<String, String> configs) {
        return new TopicState(1, 3, configs);
    }

    /// Runs a request through governance as the pipeline does: prepare, then the request,
    /// then the broker's (here empty) response.
    private void intercept(GovernanceInterceptor interceptor, ApiKeys api, Object request, Object response) {
        var prepared = interceptor.prepare(context, request(api, request));
        if (prepared != null) {
            prepared.toCompletableFuture().join();
        }
        interceptor.onRequest(context, request(api, request));
        interceptor.onResponse(context, response(api, response));
    }

    private static AlterConfigsRequestData.AlterConfigsResource alterConfigs(String topic, Map<String, String> configs) {
        var resource = new AlterConfigsRequestData.AlterConfigsResource()
                .setResourceType(ConfigResource.Type.TOPIC.id()).setResourceName(topic);
        configs.forEach((name, value) -> resource.configs().add(new AlterConfigsRequestData.AlterableConfig().setName(name).setValue(value)));
        return resource;
    }

    private static IncrementalAlterConfigsRequestData.AlterConfigsResource incrementalAlter(
            String topic, IncrementalAlterConfigsRequestData.AlterableConfig... ops) {
        var resource = new IncrementalAlterConfigsRequestData.AlterConfigsResource()
                .setResourceType(ConfigResource.Type.TOPIC.id()).setResourceName(topic);
        for (var op : ops) {
            resource.configs().add(op);
        }
        return resource;
    }

    /// @param operation 0 SET, 1 DELETE, 2 APPEND, 3 SUBTRACT
    private static IncrementalAlterConfigsRequestData.AlterableConfig op(String name, String value, int operation) {
        return new IncrementalAlterConfigsRequestData.AlterableConfig().setName(name).setValue(value).setConfigOperation((byte) operation);
    }

    private static Request request(ApiKeys api, Object body) {
        return new Request() {
            public int apiKey() {
                return api.id;
            }

            public String apiName() {
                return api.name;
            }

            public short apiVersion() {
                return api.latestVersion();
            }

            public int correlationId() {
                return 1;
            }

            public String clientId() {
                return "test";
            }

            public Object body() {
                return body;
            }
        };
    }

    private static Response response(ApiKeys api, Object body) {
        return new Response() {
            public int apiKey() {
                return api.id;
            }

            public String apiName() {
                return api.name;
            }

            public short apiVersion() {
                return api.latestVersion();
            }

            public int correlationId() {
                return 1;
            }

            public Object body() {
                return body;
            }
        };
    }
}

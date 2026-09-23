package io.jonasg.kawa.rbac;

import io.jonasg.kawa.config.AclConfig;
import io.jonasg.kawa.config.GroupConfig;
import io.jonasg.kawa.config.RbacConfig;
import io.jonasg.kawa.config.ResourceConfig;
import io.jonasg.kawa.config.RoleConfig;
import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import io.jonasg.kawa.core.Request;
import io.jonasg.kawa.core.Response;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData;
import org.apache.kafka.common.message.TxnOffsetCommitResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class TxnOffsetCommitAuthorizationCheckTest {

    private static RbacAuthorizer authorizer(AclConfig... acls) {
        return new RbacAuthorizer(new RbacConfig(
                Map.of("admin", new RoleConfig(List.of(acls))),
                Map.of("admins", new GroupConfig(List.of("alice"), List.of("admin")))));
    }

    private static AclConfig acl(ResourceType type, String name, AclOperation operation) {
        return new AclConfig(new ResourceConfig(type, name), operation);
    }

    private static GatewayContext context(String principal) {
        return new GatewayContext("source", 0L, principal);
    }

    private static Request txnOffsetCommitRequest(String transactionalId, String groupId, String topic, int... partitions) {
        var body = new TxnOffsetCommitRequestData()
                .setTransactionalId(transactionalId).setGroupId(groupId);
        var topicRequest = new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic().setName(topic);
        for (int partition : partitions) {
            topicRequest.partitions().add(new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
                    .setPartitionIndex(partition));
        }
        body.topics().add(topicRequest);
        return new TestRequest(ApiKeys.TXN_OFFSET_COMMIT.id, (short) 3, body);
    }

    private static Response txnOffsetCommitResponse(TxnOffsetCommitResponseData body) {
        return new TestResponse(ApiKeys.TXN_OFFSET_COMMIT.id, body);
    }

    private static TxnOffsetCommitResponseData brokerResponse(String topic, int... partitions) {
        var body = new TxnOffsetCommitResponseData();
        var topicResponse = new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic().setName(topic);
        for (int partition : partitions) {
            topicResponse.partitions().add(new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
                    .setPartitionIndex(partition).setErrorCode((short) 0));
        }
        body.topics().add(topicResponse);
        return body;
    }

    @Test
    void allowsTxnOffsetCommitWhenTransactionalIdWriteGroupReadAndTopicReadAreAllowed() {
        var interceptor = new AuthorizationInterceptor(
                authorizer(
                        acl(ResourceType.TRANSACTIONAL_ID, "txn-1", AclOperation.WRITE),
                        acl(ResourceType.GROUP, "g1", AclOperation.READ),
                        acl(ResourceType.TOPIC, "orders", AclOperation.READ)),
                new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var request = txnOffsetCommitRequest("txn-1", "g1", "orders", 0, 1);

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isFalse();
        var body = (TxnOffsetCommitRequestData) request.body();
        assertThat(body.topics()).hasSize(1);
        assertThat(body.topics().iterator().next().name()).isEqualTo("orders");
    }

    @Test
    void shortCircuitsWholeRequestWhenTransactionalIdWriteIsDenied() {
        var interceptor = new AuthorizationInterceptor(
                authorizer(
                        acl(ResourceType.GROUP, "g1", AclOperation.READ),
                        acl(ResourceType.TOPIC, "orders", AclOperation.READ)),
                new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var request = txnOffsetCommitRequest("txn-1", "g1", "orders", 0);

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isTrue();
        var response = (TxnOffsetCommitResponseData) ctx.shortCircuitResult().body();
        assertThat(response.topics()).hasSize(1);
        var topic = response.topics().iterator().next();
        assertThat(topic.name()).isEqualTo("orders");
        assertThat(topic.partitions().iterator().next().errorCode())
                .isEqualTo(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code());
    }

    @Test
    void shortCircuitsWholeRequestWhenGroupReadIsDenied() {
        var interceptor = new AuthorizationInterceptor(
                authorizer(
                        acl(ResourceType.TRANSACTIONAL_ID, "txn-1", AclOperation.WRITE),
                        acl(ResourceType.TOPIC, "orders", AclOperation.READ)),
                new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var request = txnOffsetCommitRequest("txn-1", "g1", "orders", 0);

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isTrue();
        var response = (TxnOffsetCommitResponseData) ctx.shortCircuitResult().body();
        assertThat(response.topics()).hasSize(1);
        var topic = response.topics().iterator().next();
        assertThat(topic.name()).isEqualTo("orders");
        assertThat(topic.partitions().iterator().next().errorCode())
                .isEqualTo(Errors.GROUP_AUTHORIZATION_FAILED.code());
    }

    @Test
    void stripsDeniedTopicsAndMergesDenialIntoResponse() {
        var interceptor = new AuthorizationInterceptor(
                authorizer(
                        acl(ResourceType.TRANSACTIONAL_ID, "txn-1", AclOperation.WRITE),
                        acl(ResourceType.GROUP, "g1", AclOperation.READ),
                        acl(ResourceType.TOPIC, "orders", AclOperation.READ)),
                new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var body = new TxnOffsetCommitRequestData()
                .setTransactionalId("txn-1").setGroupId("g1");
        body.topics().add(new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic().setName("orders")
                .setPartitions(List.of(new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition().setPartitionIndex(0))));
        body.topics().add(new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic().setName("banned")
                .setPartitions(List.of(new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition().setPartitionIndex(3))));
        var request = new TestRequest(ApiKeys.TXN_OFFSET_COMMIT.id, (short) 3, body);

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isFalse();
        assertThat(body.topics()).hasSize(1);
        assertThat(body.topics().iterator().next().name()).isEqualTo("orders");

        var response = txnOffsetCommitResponse(brokerResponse("orders", 0));
        interceptor.onResponse(ctx, response);

        var responseBody = (TxnOffsetCommitResponseData) response.body();
        assertThat(responseBody.topics()).hasSize(2);
        var banned = responseBody.topics().stream()
                .filter(t -> t.name().equals("banned")).findFirst().orElseThrow();
        assertThat(banned.partitions()).hasSize(1);
        assertThat(banned.partitions().iterator().next().partitionIndex()).isEqualTo(3);
        assertThat(banned.partitions().iterator().next().errorCode())
                .isEqualTo(Errors.TOPIC_AUTHORIZATION_FAILED.code());
    }

    @Test
    void shortCircuitsTxnOffsetCommitWhenEveryTopicIsDenied() {
        var interceptor = new AuthorizationInterceptor(
                authorizer(
                        acl(ResourceType.TRANSACTIONAL_ID, "txn-1", AclOperation.WRITE),
                        acl(ResourceType.GROUP, "g1", AclOperation.READ)),
                new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var request = txnOffsetCommitRequest("txn-1", "g1", "banned", 0, 1);

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isTrue();
        var response = (TxnOffsetCommitResponseData) ctx.shortCircuitResult().body();
        assertThat(response.topics()).hasSize(1);
        var topic = response.topics().iterator().next();
        assertThat(topic.name()).isEqualTo("banned");
        assertThat(topic.partitions().stream().map(p -> p.errorCode()).distinct().toList())
                .containsExactly(Errors.TOPIC_AUTHORIZATION_FAILED.code());
    }

    @Test
    void deniesTxnOffsetCommitWithAuthenticationFailedWhenThereIsNoPrincipal() {
        var interceptor = new AuthorizationInterceptor(authorizer(), new VirtualTopicManager(Map.of()));
        var ctx = context(null);
        var request = txnOffsetCommitRequest("txn-1", "g1", "orders", 0);

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isTrue();
        var response = (TxnOffsetCommitResponseData) ctx.shortCircuitResult().body();
        assertThat(response.topics()).hasSize(1);
        assertThat(response.topics().iterator().next().partitions().iterator().next().errorCode())
                .isEqualTo(Errors.SASL_AUTHENTICATION_FAILED.code());
    }

    private record TestRequest(int apiKey, short apiVersion, Object body) implements Request {
        @Override
        public String apiName() {
            return "test";
        }

        @Override
        public int correlationId() {
            return 42;
        }

        @Override
        public String clientId() {
            return "test";
        }
    }

    private record TestResponse(int apiKey, Object body) implements Response {
        @Override
        public String apiName() {
            return "test";
        }

        @Override
        public short apiVersion() {
            return 3;
        }

        @Override
        public int correlationId() {
            return 42;
        }
    }
}

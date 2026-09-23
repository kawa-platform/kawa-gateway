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
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData;
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class AddPartitionsToTxnAuthorizationCheckTest {

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

    private static Request addPartitionsToTxnRequest(String transactionalId, String topic, int... partitions) {
        var body = new AddPartitionsToTxnRequestData().setV3AndBelowTransactionalId(transactionalId);
        var partitionList = new java.util.ArrayList<Integer>();
        for (int partition : partitions) {
            partitionList.add(partition);
        }
        var topicRequest = new AddPartitionsToTxnRequestData.AddPartitionsToTxnTopic()
                .setName(topic).setPartitions(partitionList);
        body.v3AndBelowTopics().add(topicRequest);
        return new TestRequest(ApiKeys.ADD_PARTITIONS_TO_TXN.id, (short) 3, body);
    }

    private static Response addPartitionsToTxnResponse(AddPartitionsToTxnResponseData body) {
        return new TestResponse(ApiKeys.ADD_PARTITIONS_TO_TXN.id, body);
    }

    private static AddPartitionsToTxnResponseData brokerResponse(String topic, int... partitions) {
        var body = new AddPartitionsToTxnResponseData();
        var topicResponse = new AddPartitionsToTxnResponseData.AddPartitionsToTxnTopicResult().setName(topic);
        for (int partition : partitions) {
            topicResponse.resultsByPartition().add(new AddPartitionsToTxnResponseData.AddPartitionsToTxnPartitionResult()
                    .setPartitionIndex(partition).setPartitionErrorCode((short) 0));
        }
        body.resultsByTopicV3AndBelow().add(topicResponse);
        return body;
    }

    @Test
    void allowsAddPartitionsToTxnWhenTransactionalIdWriteAndTopicWriteAreAllowed() {
        var interceptor = new AuthorizationInterceptor(
                authorizer(
                        acl(ResourceType.TRANSACTIONAL_ID, "txn-1", AclOperation.WRITE),
                        acl(ResourceType.TOPIC, "orders", AclOperation.WRITE)),
                new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var request = addPartitionsToTxnRequest("txn-1", "orders", 0, 1);

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isFalse();
        var body = (AddPartitionsToTxnRequestData) request.body();
        assertThat(body.v3AndBelowTopics()).hasSize(1);
        assertThat(body.v3AndBelowTopics().iterator().next().name()).isEqualTo("orders");
    }

    @Test
    void shortCircuitsWholeRequestWhenTransactionalIdWriteIsDeniedEvenIfTopicWriteIsAllowed() {
        var interceptor = new AuthorizationInterceptor(
                authorizer(acl(ResourceType.TOPIC, "orders", AclOperation.WRITE)),
                new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var request = addPartitionsToTxnRequest("txn-1", "orders", 0);

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isTrue();
        var response = (AddPartitionsToTxnResponseData) ctx.shortCircuitResult().body();
        assertThat(response.resultsByTopicV3AndBelow()).hasSize(1);
        var topic = response.resultsByTopicV3AndBelow().iterator().next();
        assertThat(topic.name()).isEqualTo("orders");
        assertThat(topic.resultsByPartition().iterator().next().partitionErrorCode())
                .isEqualTo(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code());
    }

    @Test
    void stripsDeniedTopicsAndMergesDenialIntoResponse() {
        var interceptor = new AuthorizationInterceptor(
                authorizer(
                        acl(ResourceType.TRANSACTIONAL_ID, "txn-1", AclOperation.WRITE),
                        acl(ResourceType.TOPIC, "orders", AclOperation.WRITE)),
                new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var body = new AddPartitionsToTxnRequestData().setV3AndBelowTransactionalId("txn-1");
        body.v3AndBelowTopics().add(new AddPartitionsToTxnRequestData.AddPartitionsToTxnTopic()
                .setName("orders").setPartitions(List.of(0)));
        body.v3AndBelowTopics().add(new AddPartitionsToTxnRequestData.AddPartitionsToTxnTopic()
                .setName("banned").setPartitions(List.of(3)));
        var request = new TestRequest(ApiKeys.ADD_PARTITIONS_TO_TXN.id, (short) 3, body);

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isFalse();
        assertThat(body.v3AndBelowTopics()).hasSize(1);
        assertThat(body.v3AndBelowTopics().iterator().next().name()).isEqualTo("orders");

        var response = addPartitionsToTxnResponse(brokerResponse("orders", 0));
        interceptor.onResponse(ctx, response);

        var responseBody = (AddPartitionsToTxnResponseData) response.body();
        assertThat(responseBody.resultsByTopicV3AndBelow()).hasSize(2);
        var banned = responseBody.resultsByTopicV3AndBelow().stream()
                .filter(t -> t.name().equals("banned")).findFirst().orElseThrow();
        assertThat(banned.resultsByPartition()).hasSize(1);
        assertThat(banned.resultsByPartition().iterator().next().partitionIndex()).isEqualTo(3);
        assertThat(banned.resultsByPartition().iterator().next().partitionErrorCode())
                .isEqualTo(Errors.TOPIC_AUTHORIZATION_FAILED.code());
    }

    @Test
    void shortCircuitsAddPartitionsToTxnWhenEveryTopicIsDenied() {
        var interceptor = new AuthorizationInterceptor(
                authorizer(acl(ResourceType.TRANSACTIONAL_ID, "txn-1", AclOperation.WRITE)),
                new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var request = addPartitionsToTxnRequest("txn-1", "banned", 0, 1);

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isTrue();
        var response = (AddPartitionsToTxnResponseData) ctx.shortCircuitResult().body();
        assertThat(response.resultsByTopicV3AndBelow()).hasSize(1);
        var topic = response.resultsByTopicV3AndBelow().iterator().next();
        assertThat(topic.name()).isEqualTo("banned");
        assertThat(topic.resultsByPartition().stream().map(p -> p.partitionErrorCode()).distinct().toList())
                .containsExactly(Errors.TOPIC_AUTHORIZATION_FAILED.code());
    }

    @Test
    void deniesAddPartitionsToTxnWithAuthenticationFailedWhenThereIsNoPrincipal() {
        var interceptor = new AuthorizationInterceptor(authorizer(), new VirtualTopicManager(Map.of()));
        var ctx = context(null);
        var request = addPartitionsToTxnRequest("txn-1", "orders", 0);

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isTrue();
        var response = (AddPartitionsToTxnResponseData) ctx.shortCircuitResult().body();
        assertThat(response.resultsByTopicV3AndBelow()).hasSize(1);
        assertThat(response.resultsByTopicV3AndBelow().iterator().next().resultsByPartition()
                .iterator().next().partitionErrorCode())
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

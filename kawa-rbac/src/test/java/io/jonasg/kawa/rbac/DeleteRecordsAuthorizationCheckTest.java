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
import org.apache.kafka.common.message.DeleteRecordsRequestData;
import org.apache.kafka.common.message.DeleteRecordsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class DeleteRecordsAuthorizationCheckTest {

    private static RbacAuthorizer authorizer(AclConfig... acls) {
        return new RbacAuthorizer(new RbacConfig(
                Map.of("admin", new RoleConfig(List.of(acls))),
                Map.of("admins", new GroupConfig(List.of("alice"), List.of("admin")))));
    }

    private static AclConfig topicAcl(String topic, AclOperation operation) {
        return new AclConfig(new ResourceConfig(ResourceType.TOPIC, topic), operation);
    }

    private static GatewayContext context(String principal) {
        return new GatewayContext("source", 0L, principal);
    }

    private static Request deleteRecordsRequest(String topic, int... partitions) {
        var body = new DeleteRecordsRequestData();
        var deleteRecordsTopic = new DeleteRecordsRequestData.DeleteRecordsTopic().setName(topic);
        for (int partition : partitions) {
            deleteRecordsTopic.partitions().add(new DeleteRecordsRequestData.DeleteRecordsPartition()
                    .setPartitionIndex(partition));
        }
        body.topics().add(deleteRecordsTopic);
        return new TestRequest(ApiKeys.DELETE_RECORDS.id, (short) 2, body);
    }

    private static Request deleteRecordsRequest(DeleteRecordsRequestData body) {
        return new TestRequest(ApiKeys.DELETE_RECORDS.id, (short) 2, body);
    }

    private static Response deleteRecordsResponse(DeleteRecordsResponseData body) {
        return new TestResponse(ApiKeys.DELETE_RECORDS.id, body);
    }

    private static DeleteRecordsResponseData brokerResponse(String topic, int... partitions) {
        var body = new DeleteRecordsResponseData();
        var topicResponse = new DeleteRecordsResponseData.DeleteRecordsTopicResult().setName(topic);
        for (int partition : partitions) {
            topicResponse.partitions().add(new DeleteRecordsResponseData.DeleteRecordsPartitionResult()
                    .setPartitionIndex(partition).setErrorCode((short) 0).setLowWatermark(7L));
        }
        body.topics().add(topicResponse);
        return body;
    }

    @Test
    void allowsDeleteRecordsWhenTopicDeleteIsAllowed() {
        var interceptor = new AuthorizationInterceptor(
                authorizer(topicAcl("orders", AclOperation.DELETE)),
                new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var request = deleteRecordsRequest("orders", 0, 1);

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isFalse();
        var body = (DeleteRecordsRequestData) request.body();
        assertThat(body.topics()).hasSize(1);
        assertThat(body.topics().iterator().next().name()).isEqualTo("orders");
    }

    @Test
    void stripsDeniedTopicsAndMergesDenialIntoResponse() {
        var interceptor = new AuthorizationInterceptor(
                authorizer(topicAcl("orders", AclOperation.DELETE)),
                new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var body = new DeleteRecordsRequestData();
        body.topics().add(new DeleteRecordsRequestData.DeleteRecordsTopic().setName("orders")
                .setPartitions(List.of(new DeleteRecordsRequestData.DeleteRecordsPartition().setPartitionIndex(0))));
        body.topics().add(new DeleteRecordsRequestData.DeleteRecordsTopic().setName("banned")
                .setPartitions(List.of(new DeleteRecordsRequestData.DeleteRecordsPartition().setPartitionIndex(3))));
        var request = deleteRecordsRequest(body);

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isFalse();
        assertThat(body.topics()).hasSize(1);
        assertThat(body.topics().iterator().next().name()).isEqualTo("orders");

        var response = deleteRecordsResponse(brokerResponse("orders", 0));
        interceptor.onResponse(ctx, response);

        var responseBody = (DeleteRecordsResponseData) response.body();
        assertThat(responseBody.topics()).hasSize(2);
        var banned = responseBody.topics().stream()
                .filter(t -> t.name().equals("banned")).findFirst().orElseThrow();
        assertThat(banned.partitions()).hasSize(1);
        assertThat(banned.partitions().iterator().next().partitionIndex()).isEqualTo(3);
        assertThat(banned.partitions().iterator().next().errorCode())
                .isEqualTo(Errors.TOPIC_AUTHORIZATION_FAILED.code());
    }

    @Test
    void shortCircuitsDeleteRecordsWhenEveryTopicIsDenied() {
        var interceptor = new AuthorizationInterceptor(authorizer(), new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var request = deleteRecordsRequest("banned", 0, 1);

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isTrue();
        var result = ctx.shortCircuitResult();
        assertThat(result.body()).isInstanceOf(DeleteRecordsResponseData.class);
        var response = (DeleteRecordsResponseData) result.body();
        assertThat(response.topics()).hasSize(1);
        var topic = response.topics().iterator().next();
        assertThat(topic.name()).isEqualTo("banned");
        assertThat(topic.partitions()).hasSize(2);
        assertThat(topic.partitions().stream().map(p -> p.partitionIndex()).toList())
                .containsExactly(0, 1);
        assertThat(topic.partitions().stream().map(p -> p.errorCode()).distinct().toList())
                .containsExactly(Errors.TOPIC_AUTHORIZATION_FAILED.code());
    }

    @Test
    void deniesDeleteRecordsWithAuthenticationFailedWhenThereIsNoPrincipal() {
        var interceptor = new AuthorizationInterceptor(authorizer(), new VirtualTopicManager(Map.of()));
        var ctx = context(null);
        var request = deleteRecordsRequest("orders", 0);

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isTrue();
        var response = (DeleteRecordsResponseData) ctx.shortCircuitResult().body();
        assertThat(response.topics()).hasSize(1);
        assertThat(response.topics().iterator().next().partitions().iterator().next().errorCode())
                .isEqualTo(Errors.SASL_AUTHENTICATION_FAILED.code());
    }

    @Test
    void deniesDeleteRecordsWithEmptyResponseWhenBodyIsUndecoded() {
        var interceptor = new AuthorizationInterceptor(authorizer(), new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var request = new TestRequest(ApiKeys.DELETE_RECORDS.id, (short) 99, null);

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isTrue();
        var response = (DeleteRecordsResponseData) ctx.shortCircuitResult().body();
        assertThat(response.topics()).isEmpty();
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
            return 2;
        }

        @Override
        public int correlationId() {
            return 42;
        }
    }
}

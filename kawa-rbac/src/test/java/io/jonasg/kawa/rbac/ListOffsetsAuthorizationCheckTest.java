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
import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ListOffsetsAuthorizationCheckTest {

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

    private static Request listOffsetsRequest(String topic, int... partitions) {
        var body = new ListOffsetsRequestData();
        var listOffsetsTopic = new ListOffsetsRequestData.ListOffsetsTopic().setName(topic);
        for (int partition : partitions) {
            listOffsetsTopic.partitions().add(new ListOffsetsRequestData.ListOffsetsPartition()
                    .setPartitionIndex(partition));
        }
        body.topics().add(listOffsetsTopic);
        return new TestRequest(ApiKeys.LIST_OFFSETS.id, (short) 6, body);
    }

    private static Request listOffsetsRequest(ListOffsetsRequestData body) {
        return new TestRequest(ApiKeys.LIST_OFFSETS.id, (short) 6, body);
    }

    private static Response listOffsetsResponse(ListOffsetsResponseData body) {
        return new TestResponse(ApiKeys.LIST_OFFSETS.id, body);
    }

    private static ListOffsetsResponseData brokerResponse(String topic, int... partitions) {
        var body = new ListOffsetsResponseData();
        var topicResponse = new ListOffsetsResponseData.ListOffsetsTopicResponse().setName(topic);
        for (int partition : partitions) {
            topicResponse.partitions().add(new ListOffsetsResponseData.ListOffsetsPartitionResponse()
                    .setPartitionIndex(partition).setErrorCode((short) 0).setTimestamp(123L));
        }
        body.topics().add(topicResponse);
        return body;
    }

    @Test
    void allowsListOffsetsWhenTopicDescribeIsAllowed() {
        var interceptor = new AuthorizationInterceptor(
                authorizer(topicAcl("orders", AclOperation.DESCRIBE)),
                new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var request = listOffsetsRequest("orders", 0, 1);

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isFalse();
        var body = (ListOffsetsRequestData) request.body();
        assertThat(body.topics()).hasSize(1);
        assertThat(body.topics().iterator().next().name()).isEqualTo("orders");
    }

    @Test
    void stripsDeniedTopicsAndMergesDenialIntoResponse() {
        var interceptor = new AuthorizationInterceptor(
                authorizer(topicAcl("orders", AclOperation.DESCRIBE)),
                new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var body = new ListOffsetsRequestData();
        body.topics().add(new ListOffsetsRequestData.ListOffsetsTopic().setName("orders")
                .setPartitions(List.of(new ListOffsetsRequestData.ListOffsetsPartition().setPartitionIndex(0))));
        body.topics().add(new ListOffsetsRequestData.ListOffsetsTopic().setName("banned")
                .setPartitions(List.of(new ListOffsetsRequestData.ListOffsetsPartition().setPartitionIndex(3))));
        var request = listOffsetsRequest(body);

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isFalse();
        assertThat(body.topics()).hasSize(1);
        assertThat(body.topics().iterator().next().name()).isEqualTo("orders");

        var response = listOffsetsResponse(brokerResponse("orders", 0));
        interceptor.onResponse(ctx, response);

        var responseBody = (ListOffsetsResponseData) response.body();
        assertThat(responseBody.topics()).hasSize(2);
        var banned = responseBody.topics().stream()
                .filter(t -> t.name().equals("banned")).findFirst().orElseThrow();
        assertThat(banned.partitions()).hasSize(1);
        assertThat(banned.partitions().iterator().next().partitionIndex()).isEqualTo(3);
        assertThat(banned.partitions().iterator().next().errorCode())
                .isEqualTo(Errors.TOPIC_AUTHORIZATION_FAILED.code());
    }

    @Test
    void shortCircuitsListOffsetsWhenEveryTopicIsDenied() {
        var interceptor = new AuthorizationInterceptor(authorizer(), new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var request = listOffsetsRequest("banned", 0, 1);

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isTrue();
        var result = ctx.shortCircuitResult();
        assertThat(result.body()).isInstanceOf(ListOffsetsResponseData.class);
        var response = (ListOffsetsResponseData) result.body();
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
    void deniesListOffsetsWithAuthenticationFailedWhenThereIsNoPrincipal() {
        var interceptor = new AuthorizationInterceptor(authorizer(), new VirtualTopicManager(Map.of()));
        var ctx = context(null);
        var request = listOffsetsRequest("orders", 0);

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isTrue();
        var response = (ListOffsetsResponseData) ctx.shortCircuitResult().body();
        assertThat(response.topics()).hasSize(1);
        assertThat(response.topics().iterator().next().partitions().iterator().next().errorCode())
                .isEqualTo(Errors.SASL_AUTHENTICATION_FAILED.code());
    }

    @Test
    void deniesListOffsetsWithEmptyResponseWhenBodyIsUndecoded() {
        var interceptor = new AuthorizationInterceptor(authorizer(), new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var request = new TestRequest(ApiKeys.LIST_OFFSETS.id, (short) 99, null);

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isTrue();
        var response = (ListOffsetsResponseData) ctx.shortCircuitResult().body();
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
            return 6;
        }

        @Override
        public int correlationId() {
            return 42;
        }
    }
}

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
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class OffsetForLeaderEpochAuthorizationCheckTest {

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

    private static Request offsetForLeaderEpochRequest(String topic, int... partitions) {
        var body = new OffsetForLeaderEpochRequestData();
        var offsetForLeaderTopic = new OffsetForLeaderEpochRequestData.OffsetForLeaderTopic().setTopic(topic);
        for (int partition : partitions) {
            offsetForLeaderTopic.partitions().add(new OffsetForLeaderEpochRequestData.OffsetForLeaderPartition()
                    .setPartition(partition));
        }
        body.topics().add(offsetForLeaderTopic);
        return new TestRequest(ApiKeys.OFFSET_FOR_LEADER_EPOCH.id, (short) 4, body);
    }

    private static Request offsetForLeaderEpochRequest(OffsetForLeaderEpochRequestData body) {
        return new TestRequest(ApiKeys.OFFSET_FOR_LEADER_EPOCH.id, (short) 4, body);
    }

    private static Response offsetForLeaderEpochResponse(OffsetForLeaderEpochResponseData body) {
        return new TestResponse(ApiKeys.OFFSET_FOR_LEADER_EPOCH.id, body);
    }

    private static OffsetForLeaderEpochResponseData brokerResponse(String topic, int... partitions) {
        var body = new OffsetForLeaderEpochResponseData();
        var topicResponse = new OffsetForLeaderEpochResponseData.OffsetForLeaderTopicResult().setTopic(topic);
        for (int partition : partitions) {
            topicResponse.partitions().add(new OffsetForLeaderEpochResponseData.EpochEndOffset()
                    .setPartition(partition).setErrorCode((short) 0).setEndOffset(99L));
        }
        body.topics().add(topicResponse);
        return body;
    }

    @Test
    void allowsOffsetForLeaderEpochWhenTopicDescribeIsAllowed() {
        var interceptor = new AuthorizationInterceptor(
                authorizer(topicAcl("orders", AclOperation.DESCRIBE)),
                new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var request = offsetForLeaderEpochRequest("orders", 0, 1);

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isFalse();
        var body = (OffsetForLeaderEpochRequestData) request.body();
        assertThat(body.topics()).hasSize(1);
        assertThat(body.topics().iterator().next().topic()).isEqualTo("orders");
    }

    @Test
    void stripsDeniedTopicsAndMergesDenialIntoResponse() {
        var interceptor = new AuthorizationInterceptor(
                authorizer(topicAcl("orders", AclOperation.DESCRIBE)),
                new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var body = new OffsetForLeaderEpochRequestData();
        body.topics().add(new OffsetForLeaderEpochRequestData.OffsetForLeaderTopic().setTopic("orders")
                .setPartitions(List.of(new OffsetForLeaderEpochRequestData.OffsetForLeaderPartition().setPartition(0))));
        body.topics().add(new OffsetForLeaderEpochRequestData.OffsetForLeaderTopic().setTopic("banned")
                .setPartitions(List.of(new OffsetForLeaderEpochRequestData.OffsetForLeaderPartition().setPartition(3))));
        var request = offsetForLeaderEpochRequest(body);

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isFalse();
        assertThat(body.topics()).hasSize(1);
        assertThat(body.topics().iterator().next().topic()).isEqualTo("orders");

        var response = offsetForLeaderEpochResponse(brokerResponse("orders", 0));
        interceptor.onResponse(ctx, response);

        var responseBody = (OffsetForLeaderEpochResponseData) response.body();
        assertThat(responseBody.topics()).hasSize(2);
        var banned = responseBody.topics().stream()
                .filter(t -> t.topic().equals("banned")).findFirst().orElseThrow();
        assertThat(banned.partitions()).hasSize(1);
        assertThat(banned.partitions().iterator().next().partition()).isEqualTo(3);
        assertThat(banned.partitions().iterator().next().errorCode())
                .isEqualTo(Errors.TOPIC_AUTHORIZATION_FAILED.code());
    }

    @Test
    void shortCircuitsOffsetForLeaderEpochWhenEveryTopicIsDenied() {
        var interceptor = new AuthorizationInterceptor(authorizer(), new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var request = offsetForLeaderEpochRequest("banned", 0, 1);

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isTrue();
        var result = ctx.shortCircuitResult();
        assertThat(result.body()).isInstanceOf(OffsetForLeaderEpochResponseData.class);
        var response = (OffsetForLeaderEpochResponseData) result.body();
        assertThat(response.topics()).hasSize(1);
        var topic = response.topics().iterator().next();
        assertThat(topic.topic()).isEqualTo("banned");
        assertThat(topic.partitions()).hasSize(2);
        assertThat(topic.partitions().stream().map(p -> p.partition()).toList())
                .containsExactly(0, 1);
        assertThat(topic.partitions().stream().map(p -> p.errorCode()).distinct().toList())
                .containsExactly(Errors.TOPIC_AUTHORIZATION_FAILED.code());
    }

    @Test
    void deniesOffsetForLeaderEpochWithAuthenticationFailedWhenThereIsNoPrincipal() {
        var interceptor = new AuthorizationInterceptor(authorizer(), new VirtualTopicManager(Map.of()));
        var ctx = context(null);
        var request = offsetForLeaderEpochRequest("orders", 0);

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isTrue();
        var response = (OffsetForLeaderEpochResponseData) ctx.shortCircuitResult().body();
        assertThat(response.topics()).hasSize(1);
        assertThat(response.topics().iterator().next().partitions().iterator().next().errorCode())
                .isEqualTo(Errors.SASL_AUTHENTICATION_FAILED.code());
    }

    @Test
    void deniesOffsetForLeaderEpochWithEmptyResponseWhenBodyIsUndecoded() {
        var interceptor = new AuthorizationInterceptor(authorizer(), new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var request = new TestRequest(ApiKeys.OFFSET_FOR_LEADER_EPOCH.id, (short) 99, null);

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isTrue();
        var response = (OffsetForLeaderEpochResponseData) ctx.shortCircuitResult().body();
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
            return 4;
        }

        @Override
        public int correlationId() {
            return 42;
        }
    }
}

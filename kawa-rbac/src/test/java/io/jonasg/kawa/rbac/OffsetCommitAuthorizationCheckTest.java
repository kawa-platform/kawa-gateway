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
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class OffsetCommitAuthorizationCheckTest {

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

    private static Request offsetCommitRequest(String groupId, String topic, int... partitions) {
        var body = new OffsetCommitRequestData().setGroupId(groupId);
        var offsetCommitTopic = new OffsetCommitRequestData.OffsetCommitRequestTopic().setName(topic);
        for (int partition : partitions) {
            offsetCommitTopic.partitions().add(new OffsetCommitRequestData.OffsetCommitRequestPartition()
                    .setPartitionIndex(partition));
        }
        body.topics().add(offsetCommitTopic);
        return new TestRequest(ApiKeys.OFFSET_COMMIT.id, (short) 8, body);
    }

    private static Request offsetCommitRequest(OffsetCommitRequestData body) {
        return new TestRequest(ApiKeys.OFFSET_COMMIT.id, (short) 8, body);
    }

    private static Response offsetCommitResponse(OffsetCommitResponseData body) {
        return new TestResponse(ApiKeys.OFFSET_COMMIT.id, body);
    }

    private static OffsetCommitResponseData brokerResponse(String topic, int... partitions) {
        var body = new OffsetCommitResponseData();
        var topicResponse = new OffsetCommitResponseData.OffsetCommitResponseTopic().setName(topic);
        for (int partition : partitions) {
            topicResponse.partitions().add(new OffsetCommitResponseData.OffsetCommitResponsePartition()
                    .setPartitionIndex(partition).setErrorCode((short) 0));
        }
        body.topics().add(topicResponse);
        return body;
    }

    @Test
    void allowsOffsetCommitWhenGroupReadAndTopicReadAreAllowed() {
        var interceptor = new AuthorizationInterceptor(
                authorizer(
                        acl(ResourceType.GROUP, "g1", AclOperation.READ),
                        acl(ResourceType.TOPIC, "orders", AclOperation.READ)),
                new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var request = offsetCommitRequest("g1", "orders", 0, 1);

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isFalse();
        var body = (OffsetCommitRequestData) request.body();
        assertThat(body.topics()).hasSize(1);
        assertThat(body.topics().iterator().next().name()).isEqualTo("orders");
    }

    @Test
    void shortCircuitsWholeRequestWhenGroupReadIsDeniedEvenIfTopicReadIsAllowed() {
        // The GROUP READ gate is all-or-nothing: a principal with TOPIC READ allowed on every
        // topic but GROUP READ denied must still get GROUP_AUTHORIZATION_FAILED, not partial
        // success - nothing can proceed without group access.
        var interceptor = new AuthorizationInterceptor(
                authorizer(acl(ResourceType.TOPIC, "orders", AclOperation.READ)),
                new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var request = offsetCommitRequest("g1", "orders", 0);

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isTrue();
        var result = ctx.shortCircuitResult();
        assertThat(result.body()).isInstanceOf(OffsetCommitResponseData.class);
        var response = (OffsetCommitResponseData) result.body();
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
                        acl(ResourceType.GROUP, "g1", AclOperation.READ),
                        acl(ResourceType.TOPIC, "orders", AclOperation.READ)),
                new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var body = new OffsetCommitRequestData().setGroupId("g1");
        body.topics().add(new OffsetCommitRequestData.OffsetCommitRequestTopic().setName("orders")
                .setPartitions(List.of(new OffsetCommitRequestData.OffsetCommitRequestPartition().setPartitionIndex(0))));
        body.topics().add(new OffsetCommitRequestData.OffsetCommitRequestTopic().setName("banned")
                .setPartitions(List.of(new OffsetCommitRequestData.OffsetCommitRequestPartition().setPartitionIndex(3))));
        var request = offsetCommitRequest(body);

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isFalse();
        assertThat(body.topics()).hasSize(1);
        assertThat(body.topics().iterator().next().name()).isEqualTo("orders");

        var response = offsetCommitResponse(brokerResponse("orders", 0));
        interceptor.onResponse(ctx, response);

        var responseBody = (OffsetCommitResponseData) response.body();
        assertThat(responseBody.topics()).hasSize(2);
        var banned = responseBody.topics().stream()
                .filter(t -> t.name().equals("banned")).findFirst().orElseThrow();
        assertThat(banned.partitions()).hasSize(1);
        assertThat(banned.partitions().iterator().next().partitionIndex()).isEqualTo(3);
        assertThat(banned.partitions().iterator().next().errorCode())
                .isEqualTo(Errors.TOPIC_AUTHORIZATION_FAILED.code());
    }

    @Test
    void shortCircuitsOffsetCommitWhenEveryTopicIsDenied() {
        var interceptor = new AuthorizationInterceptor(
                authorizer(acl(ResourceType.GROUP, "g1", AclOperation.READ)),
                new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var request = offsetCommitRequest("g1", "banned", 0, 1);

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isTrue();
        var response = (OffsetCommitResponseData) ctx.shortCircuitResult().body();
        assertThat(response.topics()).hasSize(1);
        var topic = response.topics().iterator().next();
        assertThat(topic.name()).isEqualTo("banned");
        assertThat(topic.partitions().stream().map(p -> p.errorCode()).distinct().toList())
                .containsExactly(Errors.TOPIC_AUTHORIZATION_FAILED.code());
    }

    @Test
    void deniesOffsetCommitWithAuthenticationFailedWhenThereIsNoPrincipal() {
        var interceptor = new AuthorizationInterceptor(authorizer(), new VirtualTopicManager(Map.of()));
        var ctx = context(null);
        var request = offsetCommitRequest("g1", "orders", 0);

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isTrue();
        var response = (OffsetCommitResponseData) ctx.shortCircuitResult().body();
        assertThat(response.topics()).hasSize(1);
        assertThat(response.topics().iterator().next().partitions().iterator().next().errorCode())
                .isEqualTo(Errors.SASL_AUTHENTICATION_FAILED.code());
    }

    @Test
    void deniesOffsetCommitWithEmptyResponseWhenBodyIsUndecoded() {
        var interceptor = new AuthorizationInterceptor(authorizer(), new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var request = new TestRequest(ApiKeys.OFFSET_COMMIT.id, (short) 99, null);

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isTrue();
        var response = (OffsetCommitResponseData) ctx.shortCircuitResult().body();
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
            return 8;
        }

        @Override
        public int correlationId() {
            return 42;
        }
    }
}

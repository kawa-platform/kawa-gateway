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
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class OffsetFetchAuthorizationCheckTest {

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

    private static Request offsetFetchRequest(String groupId, String... topics) {
        var body = new OffsetFetchRequestData().setGroupId(groupId);
        for (String topic : topics) {
            body.topics().add(new OffsetFetchRequestData.OffsetFetchRequestTopic().setName(topic));
        }
        return new TestRequest(ApiKeys.OFFSET_FETCH.id, (short) 8, body);
    }

    private static Request offsetFetchRequest(OffsetFetchRequestData body) {
        return new TestRequest(ApiKeys.OFFSET_FETCH.id, (short) 8, body);
    }

    private static Response offsetFetchResponse(OffsetFetchResponseData body) {
        return new TestResponse(ApiKeys.OFFSET_FETCH.id, body);
    }

    private static OffsetFetchResponseData brokerResponse(String topic) {
        var body = new OffsetFetchResponseData();
        var topicResponse = new OffsetFetchResponseData.OffsetFetchResponseTopic().setName(topic);
        topicResponse.partitions().add(new OffsetFetchResponseData.OffsetFetchResponsePartition()
                .setPartitionIndex(0).setErrorCode((short) 0).setCommittedOffset(42L));
        body.topics().add(topicResponse);
        return body;
    }

    @Test
    void allowsOffsetFetchWhenGroupDescribeAndTopicDescribeAreAllowed() {
        var interceptor = new AuthorizationInterceptor(
                authorizer(
                        acl(ResourceType.GROUP, "g1", AclOperation.DESCRIBE),
                        acl(ResourceType.TOPIC, "orders", AclOperation.DESCRIBE)),
                new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var request = offsetFetchRequest("g1", "orders");

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isFalse();
        var body = (OffsetFetchRequestData) request.body();
        assertThat(body.topics()).hasSize(1);
        assertThat(body.topics().iterator().next().name()).isEqualTo("orders");
    }

    @Test
    void shortCircuitsWholeRequestWhenGroupDescribeIsDeniedEvenIfTopicDescribeIsAllowed() {
        // The GROUP DESCRIBE gate is all-or-nothing: a principal with TOPIC DESCRIBE allowed on
        // every topic but GROUP DESCRIBE denied must still get GROUP_AUTHORIZATION_FAILED, not
        // partial success - nothing can proceed without group access.
        var interceptor = new AuthorizationInterceptor(
                authorizer(acl(ResourceType.TOPIC, "orders", AclOperation.DESCRIBE)),
                new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var request = offsetFetchRequest("g1", "orders");

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isTrue();
        var result = ctx.shortCircuitResult();
        assertThat(result.body()).isInstanceOf(OffsetFetchResponseData.class);
        var response = (OffsetFetchResponseData) result.body();
        assertThat(response.errorCode()).isEqualTo(Errors.GROUP_AUTHORIZATION_FAILED.code());
    }

    @Test
    void stripsDeniedTopicsAndMergesDenialIntoResponse() {
        var interceptor = new AuthorizationInterceptor(
                authorizer(
                        acl(ResourceType.GROUP, "g1", AclOperation.DESCRIBE),
                        acl(ResourceType.TOPIC, "orders", AclOperation.DESCRIBE)),
                new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var request = offsetFetchRequest("g1", "orders", "banned");

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isFalse();
        var body = (OffsetFetchRequestData) request.body();
        assertThat(body.topics()).hasSize(1);
        assertThat(body.topics().iterator().next().name()).isEqualTo("orders");

        var response = offsetFetchResponse(brokerResponse("orders"));
        interceptor.onResponse(ctx, response);

        var responseBody = (OffsetFetchResponseData) response.body();
        assertThat(responseBody.topics()).hasSize(2);
        var banned = responseBody.topics().stream()
                .filter(t -> t.name().equals("banned")).findFirst().orElseThrow();
        assertThat(banned.partitions()).hasSize(1);
        assertThat(banned.partitions().iterator().next().errorCode())
                .isEqualTo(Errors.TOPIC_AUTHORIZATION_FAILED.code());
    }

    @Test
    void shortCircuitsOffsetFetchWhenEveryTopicIsDenied() {
        var interceptor = new AuthorizationInterceptor(
                authorizer(acl(ResourceType.GROUP, "g1", AclOperation.DESCRIBE)),
                new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var request = offsetFetchRequest("g1", "banned");

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isTrue();
        var response = (OffsetFetchResponseData) ctx.shortCircuitResult().body();
        assertThat(response.topics()).hasSize(1);
        var topic = response.topics().iterator().next();
        assertThat(topic.name()).isEqualTo("banned");
        assertThat(topic.partitions().iterator().next().errorCode())
                .isEqualTo(Errors.TOPIC_AUTHORIZATION_FAILED.code());
    }

    @Test
    void forwardsFetchAllUnchangedWhenTopicsIsNull() {
        // topics() == null means "fetch all offsets for this group". Forward unchanged - no
        // per-topic filtering is possible in this slice (known gap, documented in the check).
        var interceptor = new AuthorizationInterceptor(
                authorizer(acl(ResourceType.GROUP, "g1", AclOperation.DESCRIBE)),
                new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var body = new OffsetFetchRequestData().setGroupId("g1").setTopics(null);
        var request = offsetFetchRequest(body);

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isFalse();
        assertThat(((OffsetFetchRequestData) request.body()).topics()).isNull();
    }

    @Test
    void deniesOffsetFetchWithAuthenticationFailedWhenThereIsNoPrincipal() {
        var interceptor = new AuthorizationInterceptor(authorizer(), new VirtualTopicManager(Map.of()));
        var ctx = context(null);
        var request = offsetFetchRequest("g1", "orders");

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isTrue();
        var response = (OffsetFetchResponseData) ctx.shortCircuitResult().body();
        assertThat(response.errorCode()).isEqualTo(Errors.SASL_AUTHENTICATION_FAILED.code());
    }

    @Test
    void deniesOffsetFetchWithEmptyResponseWhenBodyIsUndecoded() {
        var interceptor = new AuthorizationInterceptor(authorizer(), new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var request = new TestRequest(ApiKeys.OFFSET_FETCH.id, (short) 99, null);

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isTrue();
        var response = (OffsetFetchResponseData) ctx.shortCircuitResult().body();
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

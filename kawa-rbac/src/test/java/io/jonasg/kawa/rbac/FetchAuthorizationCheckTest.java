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
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class FetchAuthorizationCheckTest {

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

    private static Request fetchRequest(String topic, int... partitions) {
        var body = new FetchRequestData();
        var fetchTopic = new FetchRequestData.FetchTopic().setTopic(topic);
        for (int partition : partitions) {
            fetchTopic.partitions().add(new FetchRequestData.FetchPartition().setPartition(partition));
        }
        body.topics().add(fetchTopic);
        return new TestRequest(ApiKeys.FETCH.id, (short) 16, body);
    }

    private static Request fetchRequest(FetchRequestData body) {
        return new TestRequest(ApiKeys.FETCH.id, (short) 16, body);
    }

    private static Response fetchResponse(FetchResponseData body) {
        return new TestResponse(ApiKeys.FETCH.id, body);
    }

    private static FetchResponseData brokerResponse(String topic, int... partitions) {
        var body = new FetchResponseData();
        var topicResponse = new FetchResponseData.FetchableTopicResponse().setTopic(topic);
        for (int partition : partitions) {
            topicResponse.partitions().add(new FetchResponseData.PartitionData()
                    .setPartitionIndex(partition).setErrorCode((short) 0).setHighWatermark(42L));
        }
        body.responses().add(topicResponse);
        return body;
    }

    @Test
    void allowsFetchWhenTopicReadIsAllowed() {
        var interceptor = new AuthorizationInterceptor(
                authorizer(topicAcl("orders", AclOperation.READ)),
                new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var request = fetchRequest("orders", 0, 1);

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isFalse();
        var body = (FetchRequestData) request.body();
        assertThat(body.topics()).hasSize(1);
        assertThat(body.topics().iterator().next().topic()).isEqualTo("orders");
    }

    @Test
    void stripsDeniedTopicsAndMergesDenialIntoResponse() {
        var interceptor = new AuthorizationInterceptor(
                authorizer(topicAcl("orders", AclOperation.READ)),
                new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var body = new FetchRequestData();
        body.topics().add(new FetchRequestData.FetchTopic().setTopic("orders")
                .setPartitions(List.of(new FetchRequestData.FetchPartition().setPartition(0))));
        body.topics().add(new FetchRequestData.FetchTopic().setTopic("banned")
                .setPartitions(List.of(new FetchRequestData.FetchPartition().setPartition(3))));
        var request = fetchRequest(body);

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isFalse();
        assertThat(body.topics()).hasSize(1);
        assertThat(body.topics().iterator().next().topic()).isEqualTo("orders");

        var response = fetchResponse(brokerResponse("orders", 0));
        interceptor.onResponse(ctx, response);

        var responseBody = (FetchResponseData) response.body();
        assertThat(responseBody.responses()).hasSize(2);
        var banned = responseBody.responses().stream()
                .filter(t -> t.topic().equals("banned")).findFirst().orElseThrow();
        assertThat(banned.partitions()).hasSize(1);
        assertThat(banned.partitions().iterator().next().partitionIndex()).isEqualTo(3);
        assertThat(banned.partitions().iterator().next().errorCode())
                .isEqualTo(Errors.TOPIC_AUTHORIZATION_FAILED.code());
    }

    @Test
    void shortCircuitsFetchWhenEveryTopicIsDenied() {
        var interceptor = new AuthorizationInterceptor(authorizer(), new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var request = fetchRequest("banned", 0, 1);

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isTrue();
        var result = ctx.shortCircuitResult();
        assertThat(result.body()).isInstanceOf(FetchResponseData.class);
        var response = (FetchResponseData) result.body();
        assertThat(response.responses()).hasSize(1);
        var topic = response.responses().iterator().next();
        assertThat(topic.topic()).isEqualTo("banned");
        assertThat(topic.partitions()).hasSize(2);
        assertThat(topic.partitions().stream().map(p -> p.partitionIndex()).toList())
                .containsExactly(0, 1);
        assertThat(topic.partitions().stream().map(p -> p.errorCode()).distinct().toList())
                .containsExactly(Errors.TOPIC_AUTHORIZATION_FAILED.code());
    }

    @Test
    void deniesFetchWithAuthenticationFailedWhenThereIsNoPrincipal() {
        var interceptor = new AuthorizationInterceptor(authorizer(), new VirtualTopicManager(Map.of()));
        var ctx = context(null);
        var request = fetchRequest("orders", 0);

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isTrue();
        var response = (FetchResponseData) ctx.shortCircuitResult().body();
        assertThat(response.responses()).hasSize(1);
        assertThat(response.responses().iterator().next().partitions().iterator().next().errorCode())
                .isEqualTo(Errors.SASL_AUTHENTICATION_FAILED.code());
    }

    @Test
    void deniesFetchWithEmptyResponseWhenBodyIsUndecoded() {
        var interceptor = new AuthorizationInterceptor(authorizer(), new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var request = new TestRequest(ApiKeys.FETCH.id, (short) 99, null);

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isTrue();
        var response = (FetchResponseData) ctx.shortCircuitResult().body();
        assertThat(response.responses()).isEmpty();
    }

    @Test
    void doesNotShortCircuitEmptyFetchWithNothingDenied() {
        // Steady-state incremental fetch: the client omits topics already part of the session,
        // so an empty topics() list is the normal case, not an edge case. Nothing was denied in
        // this request, so it must pass through unmodified rather than short-circuit.
        var interceptor = new AuthorizationInterceptor(authorizer(), new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var request = fetchRequest(new FetchRequestData());

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isFalse();
        assertThat(((FetchRequestData) request.body()).topics()).isEmpty();
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
            return 16;
        }

        @Override
        public int correlationId() {
            return 42;
        }
    }
}

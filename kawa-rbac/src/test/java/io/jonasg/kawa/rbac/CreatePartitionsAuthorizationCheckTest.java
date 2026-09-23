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
import org.apache.kafka.common.message.CreatePartitionsRequestData;
import org.apache.kafka.common.message.CreatePartitionsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class CreatePartitionsAuthorizationCheckTest {

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

    private static Request createPartitionsRequest(String topic) {
        var body = new CreatePartitionsRequestData();
        body.topics().add(new CreatePartitionsRequestData.CreatePartitionsTopic().setName(topic));
        return new TestRequest(ApiKeys.CREATE_PARTITIONS.id, (short) 3, body);
    }

    private static Response createPartitionsResponse(CreatePartitionsResponseData body) {
        return new TestResponse(ApiKeys.CREATE_PARTITIONS.id, body);
    }

    private static CreatePartitionsResponseData brokerResponse(String topic) {
        var body = new CreatePartitionsResponseData();
        body.results().add(new CreatePartitionsResponseData.CreatePartitionsTopicResult()
                .setName(topic).setErrorCode((short) 0));
        return body;
    }

    @Test
    void allowsCreatePartitionsWhenTopicAlterIsAllowed() {
        var interceptor = new AuthorizationInterceptor(
                authorizer(acl(ResourceType.TOPIC, "orders", AclOperation.ALTER)),
                new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var request = createPartitionsRequest("orders");

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isFalse();
        var body = (CreatePartitionsRequestData) request.body();
        assertThat(body.topics()).hasSize(1);
        assertThat(body.topics().iterator().next().name()).isEqualTo("orders");
    }

    @Test
    void stripsDeniedTopicsAndMergesDenialIntoResponse() {
        var interceptor = new AuthorizationInterceptor(
                authorizer(acl(ResourceType.TOPIC, "orders", AclOperation.ALTER)),
                new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var body = new CreatePartitionsRequestData();
        body.topics().add(new CreatePartitionsRequestData.CreatePartitionsTopic().setName("orders"));
        body.topics().add(new CreatePartitionsRequestData.CreatePartitionsTopic().setName("banned"));
        var request = new TestRequest(ApiKeys.CREATE_PARTITIONS.id, (short) 3, body);

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isFalse();
        assertThat(body.topics()).hasSize(1);
        assertThat(body.topics().iterator().next().name()).isEqualTo("orders");

        var response = createPartitionsResponse(brokerResponse("orders"));
        interceptor.onResponse(ctx, response);

        var responseBody = (CreatePartitionsResponseData) response.body();
        assertThat(responseBody.results()).hasSize(2);
        var banned = responseBody.results().stream()
                .filter(t -> t.name().equals("banned")).findFirst().orElseThrow();
        assertThat(banned.errorCode()).isEqualTo(Errors.TOPIC_AUTHORIZATION_FAILED.code());
    }

    @Test
    void shortCircuitsCreatePartitionsWhenEveryTopicIsDenied() {
        var interceptor = new AuthorizationInterceptor(
                authorizer(acl(ResourceType.TOPIC, "orders", AclOperation.ALTER)),
                new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var request = createPartitionsRequest("banned");

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isTrue();
        var response = (CreatePartitionsResponseData) ctx.shortCircuitResult().body();
        assertThat(response.results()).hasSize(1);
        assertThat(response.results().iterator().next().name()).isEqualTo("banned");
        assertThat(response.results().iterator().next().errorCode())
                .isEqualTo(Errors.TOPIC_AUTHORIZATION_FAILED.code());
    }

    @Test
    void deniesCreatePartitionsWithAuthenticationFailedWhenThereIsNoPrincipal() {
        var interceptor = new AuthorizationInterceptor(authorizer(), new VirtualTopicManager(Map.of()));
        var ctx = context(null);
        var request = createPartitionsRequest("orders");

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isTrue();
        var response = (CreatePartitionsResponseData) ctx.shortCircuitResult().body();
        assertThat(response.results()).hasSize(1);
        assertThat(response.results().iterator().next().errorCode())
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

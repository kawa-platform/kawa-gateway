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
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.message.AlterConfigsRequestData;
import org.apache.kafka.common.message.AlterConfigsResponseData;
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData;
import org.apache.kafka.common.message.IncrementalAlterConfigsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class AlterConfigsAuthorizationCheckTest {

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

    private static AlterConfigsRequestData.AlterConfigsResource topicResource(String name) {
        return new AlterConfigsRequestData.AlterConfigsResource()
                .setResourceType(ConfigResource.Type.TOPIC.id())
                .setResourceName(name);
    }

    private static AlterConfigsRequestData.AlterConfigsResource brokerResource() {
        return new AlterConfigsRequestData.AlterConfigsResource()
                .setResourceType(ConfigResource.Type.BROKER.id())
                .setResourceName("0");
    }

    private static IncrementalAlterConfigsRequestData.AlterConfigsResource incrementalTopicResource(String name) {
        return new IncrementalAlterConfigsRequestData.AlterConfigsResource()
                .setResourceType(ConfigResource.Type.TOPIC.id())
                .setResourceName(name);
    }

    @Test
    void allowsAlterConfigsWhenTopicAlterConfigsIsAllowed() {
        var interceptor = new AuthorizationInterceptor(
                authorizer(topicAcl("orders", AclOperation.ALTER_CONFIGS)),
                new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var body = new AlterConfigsRequestData();
        body.resources().add(topicResource("orders"));
        var request = new TestRequest(ApiKeys.ALTER_CONFIGS.id, (short) 2, body);

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isFalse();
        var requestBody = (AlterConfigsRequestData) request.body();
        assertThat(requestBody.resources()).hasSize(1);
        assertThat(requestBody.resources().iterator().next().resourceName()).isEqualTo("orders");
    }

    @Test
    void stripsDeniedTopicResourcesAndMergesDenialIntoResponse() {
        var interceptor = new AuthorizationInterceptor(
                authorizer(topicAcl("orders", AclOperation.ALTER_CONFIGS)),
                new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var body = new AlterConfigsRequestData();
        body.resources().add(topicResource("orders"));
        body.resources().add(topicResource("banned"));
        var request = new TestRequest(ApiKeys.ALTER_CONFIGS.id, (short) 2, body);

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isFalse();
        var requestBody = (AlterConfigsRequestData) request.body();
        assertThat(requestBody.resources()).hasSize(1);
        assertThat(requestBody.resources().iterator().next().resourceName()).isEqualTo("orders");

        var responseBody = new AlterConfigsResponseData();
        responseBody.responses().add(new AlterConfigsResponseData.AlterConfigsResourceResponse()
                .setResourceType(ConfigResource.Type.TOPIC.id())
                .setResourceName("orders")
                .setErrorCode((short) 0));
        var response = new TestResponse(ApiKeys.ALTER_CONFIGS.id, responseBody);
        interceptor.onResponse(ctx, response);

        var responseData = (AlterConfigsResponseData) response.body();
        assertThat(responseData.responses()).hasSize(2);
        var banned = responseData.responses().stream()
                .filter(r -> r.resourceName().equals("banned")).findFirst().orElseThrow();
        assertThat(banned.resourceType()).isEqualTo(ConfigResource.Type.TOPIC.id());
        assertThat(banned.errorCode()).isEqualTo(Errors.TOPIC_AUTHORIZATION_FAILED.code());
    }

    @Test
    void leavesNonTopicResourcesUntouched() {
        var interceptor = new AuthorizationInterceptor(authorizer(), new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var body = new AlterConfigsRequestData();
        body.resources().add(brokerResource());
        var request = new TestRequest(ApiKeys.ALTER_CONFIGS.id, (short) 2, body);

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isFalse();
        var requestBody = (AlterConfigsRequestData) request.body();
        assertThat(requestBody.resources()).hasSize(1);
        assertThat(requestBody.resources().iterator().next().resourceType()).isEqualTo(ConfigResource.Type.BROKER.id());
    }

    @Test
    void shortCircuitsAlterConfigsWhenEveryTopicResourceIsDenied() {
        var interceptor = new AuthorizationInterceptor(authorizer(), new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var body = new AlterConfigsRequestData();
        body.resources().add(topicResource("banned"));
        var request = new TestRequest(ApiKeys.ALTER_CONFIGS.id, (short) 2, body);

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isTrue();
        var result = ctx.shortCircuitResult();
        assertThat(result.body()).isInstanceOf(AlterConfigsResponseData.class);
        var response = (AlterConfigsResponseData) result.body();
        assertThat(response.responses()).hasSize(1);
        var denied = response.responses().iterator().next();
        assertThat(denied.resourceName()).isEqualTo("banned");
        assertThat(denied.resourceType()).isEqualTo(ConfigResource.Type.TOPIC.id());
        assertThat(denied.errorCode()).isEqualTo(Errors.TOPIC_AUTHORIZATION_FAILED.code());
    }

    @Test
    void deniesAlterConfigsWithAuthenticationFailedWhenThereIsNoPrincipal() {
        var interceptor = new AuthorizationInterceptor(authorizer(), new VirtualTopicManager(Map.of()));
        var ctx = context(null);
        var body = new AlterConfigsRequestData();
        body.resources().add(topicResource("orders"));
        var request = new TestRequest(ApiKeys.ALTER_CONFIGS.id, (short) 2, body);

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isTrue();
        var response = (AlterConfigsResponseData) ctx.shortCircuitResult().body();
        assertThat(response.responses()).hasSize(1);
        assertThat(response.responses().iterator().next().errorCode())
                .isEqualTo(Errors.SASL_AUTHENTICATION_FAILED.code());
    }

    @Test
    void deniesAlterConfigsWithEmptyResponseWhenBodyIsUndecoded() {
        var interceptor = new AuthorizationInterceptor(authorizer(), new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var request = new TestRequest(ApiKeys.ALTER_CONFIGS.id, (short) 99, null);

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isTrue();
        var response = (AlterConfigsResponseData) ctx.shortCircuitResult().body();
        assertThat(response.responses()).isEmpty();
    }

    @Test
    void gatesIncrementalAlterConfigsWithSameShape() {
        var interceptor = new AuthorizationInterceptor(
                authorizer(topicAcl("orders", AclOperation.ALTER_CONFIGS)),
                new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var body = new IncrementalAlterConfigsRequestData();
        body.resources().add(incrementalTopicResource("orders"));
        body.resources().add(incrementalTopicResource("banned"));
        var request = new TestRequest(ApiKeys.INCREMENTAL_ALTER_CONFIGS.id, (short) 1, body);

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isFalse();
        var requestBody = (IncrementalAlterConfigsRequestData) request.body();
        assertThat(requestBody.resources()).hasSize(1);
        assertThat(requestBody.resources().iterator().next().resourceName()).isEqualTo("orders");

        var responseBody = new IncrementalAlterConfigsResponseData();
        responseBody.responses().add(new IncrementalAlterConfigsResponseData.AlterConfigsResourceResponse()
                .setResourceType(ConfigResource.Type.TOPIC.id())
                .setResourceName("orders")
                .setErrorCode((short) 0));
        var response = new TestResponse(ApiKeys.INCREMENTAL_ALTER_CONFIGS.id, responseBody);
        interceptor.onResponse(ctx, response);

        var responseData = (IncrementalAlterConfigsResponseData) response.body();
        assertThat(responseData.responses()).hasSize(2);
        var banned = responseData.responses().stream()
                .filter(r -> r.resourceName().equals("banned")).findFirst().orElseThrow();
        assertThat(banned.resourceType()).isEqualTo(ConfigResource.Type.TOPIC.id());
        assertThat(banned.errorCode()).isEqualTo(Errors.TOPIC_AUTHORIZATION_FAILED.code());
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

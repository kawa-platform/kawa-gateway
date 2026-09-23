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
import org.apache.kafka.common.message.DescribeConfigsRequestData;
import org.apache.kafka.common.message.DescribeConfigsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class DescribeConfigsAuthorizationCheckTest {

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

    private static Request describeConfigsRequest(DescribeConfigsRequestData.DescribeConfigsResource... resources) {
        var body = new DescribeConfigsRequestData();
        for (var resource : resources) {
            body.resources().add(resource);
        }
        return new TestRequest(ApiKeys.DESCRIBE_CONFIGS.id, (short) 4, body);
    }

    private static DescribeConfigsRequestData.DescribeConfigsResource topicResource(String name) {
        return new DescribeConfigsRequestData.DescribeConfigsResource()
                .setResourceType(ConfigResource.Type.TOPIC.id())
                .setResourceName(name);
    }

    private static DescribeConfigsRequestData.DescribeConfigsResource brokerResource() {
        return new DescribeConfigsRequestData.DescribeConfigsResource()
                .setResourceType(ConfigResource.Type.BROKER.id())
                .setResourceName("0");
    }

    private static Response describeConfigsResponse(DescribeConfigsResponseData body) {
        return new TestResponse(ApiKeys.DESCRIBE_CONFIGS.id, body);
    }

    private static DescribeConfigsResponseData brokerResponse(String topic) {
        var body = new DescribeConfigsResponseData();
        body.results().add(new DescribeConfigsResponseData.DescribeConfigsResult()
                .setResourceType(ConfigResource.Type.TOPIC.id())
                .setResourceName(topic)
                .setErrorCode((short) 0));
        return body;
    }

    @Test
    void allowsDescribeConfigsWhenTopicDescribeConfigsIsAllowed() {
        var interceptor = new AuthorizationInterceptor(
                authorizer(topicAcl("orders", AclOperation.DESCRIBE_CONFIGS)),
                new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var request = describeConfigsRequest(topicResource("orders"));

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isFalse();
        var body = (DescribeConfigsRequestData) request.body();
        assertThat(body.resources()).hasSize(1);
        assertThat(body.resources().iterator().next().resourceName()).isEqualTo("orders");
    }

    @Test
    void stripsDeniedTopicResourcesAndMergesDenialIntoResponse() {
        var interceptor = new AuthorizationInterceptor(
                authorizer(topicAcl("orders", AclOperation.DESCRIBE_CONFIGS)),
                new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var request = describeConfigsRequest(topicResource("orders"), topicResource("banned"));

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isFalse();
        var body = (DescribeConfigsRequestData) request.body();
        assertThat(body.resources()).hasSize(1);
        assertThat(body.resources().iterator().next().resourceName()).isEqualTo("orders");

        var response = describeConfigsResponse(brokerResponse("orders"));
        interceptor.onResponse(ctx, response);

        var responseBody = (DescribeConfigsResponseData) response.body();
        assertThat(responseBody.results()).hasSize(2);
        var banned = responseBody.results().stream()
                .filter(r -> r.resourceName().equals("banned")).findFirst().orElseThrow();
        assertThat(banned.resourceType()).isEqualTo(ConfigResource.Type.TOPIC.id());
        assertThat(banned.errorCode()).isEqualTo(Errors.TOPIC_AUTHORIZATION_FAILED.code());
    }

    @Test
    void leavesNonTopicResourcesUntouched() {
        var interceptor = new AuthorizationInterceptor(authorizer(), new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var request = describeConfigsRequest(brokerResource());

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isFalse();
        var body = (DescribeConfigsRequestData) request.body();
        assertThat(body.resources()).hasSize(1);
        assertThat(body.resources().iterator().next().resourceType()).isEqualTo(ConfigResource.Type.BROKER.id());
    }

    @Test
    void shortCircuitsDescribeConfigsWhenEveryTopicResourceIsDenied() {
        var interceptor = new AuthorizationInterceptor(authorizer(), new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var request = describeConfigsRequest(topicResource("banned"));

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isTrue();
        var result = ctx.shortCircuitResult();
        assertThat(result.body()).isInstanceOf(DescribeConfigsResponseData.class);
        var response = (DescribeConfigsResponseData) result.body();
        assertThat(response.results()).hasSize(1);
        var denied = response.results().iterator().next();
        assertThat(denied.resourceName()).isEqualTo("banned");
        assertThat(denied.resourceType()).isEqualTo(ConfigResource.Type.TOPIC.id());
        assertThat(denied.errorCode()).isEqualTo(Errors.TOPIC_AUTHORIZATION_FAILED.code());
    }

    @Test
    void deniesDescribeConfigsWithAuthenticationFailedWhenThereIsNoPrincipal() {
        var interceptor = new AuthorizationInterceptor(authorizer(), new VirtualTopicManager(Map.of()));
        var ctx = context(null);
        var request = describeConfigsRequest(topicResource("orders"));

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isTrue();
        var response = (DescribeConfigsResponseData) ctx.shortCircuitResult().body();
        assertThat(response.results()).hasSize(1);
        assertThat(response.results().iterator().next().errorCode())
                .isEqualTo(Errors.SASL_AUTHENTICATION_FAILED.code());
    }

    @Test
    void deniesDescribeConfigsWithEmptyResponseWhenBodyIsUndecoded() {
        var interceptor = new AuthorizationInterceptor(authorizer(), new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var request = new TestRequest(ApiKeys.DESCRIBE_CONFIGS.id, (short) 99, null);

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isTrue();
        var response = (DescribeConfigsResponseData) ctx.shortCircuitResult().body();
        assertThat(response.results()).isEmpty();
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

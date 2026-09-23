package io.jonasg.kawa.rbac;

import io.jonasg.kawa.config.AclConfig;
import io.jonasg.kawa.config.GroupConfig;
import io.jonasg.kawa.config.RbacConfig;
import io.jonasg.kawa.config.ResourceConfig;
import io.jonasg.kawa.config.RoleConfig;
import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.core.Request;
import io.jonasg.kawa.core.Response;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.message.DescribeGroupsRequestData;
import org.apache.kafka.common.message.DescribeGroupsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class DescribeGroupsAuthorizationCheckTest {

    private static RbacAuthorizer authorizer(AclConfig... acls) {
        return new RbacAuthorizer(new RbacConfig(
                Map.of("admin", new RoleConfig(List.of(acls))),
                Map.of("admins", new GroupConfig(List.of("alice"), List.of("admin")))));
    }

    private static AclConfig groupAcl(String group) {
        return new AclConfig(new ResourceConfig(ResourceType.GROUP, group), AclOperation.DESCRIBE);
    }

    private static GatewayContext context(String principal) {
        return new GatewayContext("source", 0L, principal);
    }

    private static Request describeGroupsRequest(String... groups) {
        var body = new DescribeGroupsRequestData();
        for (String group : groups) {
            body.groups().add(group);
        }
        return new TestRequest(ApiKeys.DESCRIBE_GROUPS.id, (short) 5, body);
    }

    private static Request describeGroupsRequest(DescribeGroupsRequestData body) {
        return new TestRequest(ApiKeys.DESCRIBE_GROUPS.id, (short) 5, body);
    }

    private static Response describeGroupsResponse(DescribeGroupsResponseData body) {
        return new TestResponse(ApiKeys.DESCRIBE_GROUPS.id, body);
    }

    private static DescribeGroupsResponseData brokerResponse(String... groups) {
        var body = new DescribeGroupsResponseData();
        for (String group : groups) {
            body.groups().add(new DescribeGroupsResponseData.DescribedGroup()
                    .setGroupId(group).setErrorCode((short) 0));
        }
        return body;
    }

    @Test
    void stripsDeniedGroupsAndMergesDenialIntoResponse() {
        var interceptor = new AuthorizationInterceptor(
                authorizer(groupAcl("orders-group")),
                new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var request = describeGroupsRequest("orders-group", "banned-group");

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isFalse();
        var body = (DescribeGroupsRequestData) request.body();
        assertThat(body.groups()).containsExactly("orders-group");

        var response = describeGroupsResponse(brokerResponse("orders-group"));
        interceptor.onResponse(ctx, response);

        var responseBody = (DescribeGroupsResponseData) response.body();
        assertThat(responseBody.groups()).hasSize(2);
        var banned = responseBody.groups().stream()
                .filter(g -> g.groupId().equals("banned-group")).findFirst().orElseThrow();
        assertThat(banned.errorCode()).isEqualTo(Errors.GROUP_AUTHORIZATION_FAILED.code());
    }

    @Test
    void shortCircuitsDescribeGroupsWhenEveryGroupIsDenied() {
        var interceptor = new AuthorizationInterceptor(authorizer(),
                new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var request = describeGroupsRequest("banned-group");

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isTrue();
        var result = ctx.shortCircuitResult();
        assertThat(result.body()).isInstanceOf(DescribeGroupsResponseData.class);
        var response = (DescribeGroupsResponseData) result.body();
        assertThat(response.groups()).hasSize(1);
        assertThat(response.groups().iterator().next().groupId()).isEqualTo("banned-group");
        assertThat(response.groups().iterator().next().errorCode())
                .isEqualTo(Errors.GROUP_AUTHORIZATION_FAILED.code());
    }

    @Test
    void deniesDescribeGroupsWithAuthenticationFailedWhenThereIsNoPrincipal() {
        var interceptor = new AuthorizationInterceptor(authorizer(),
                new VirtualTopicManager(Map.of()));
        var ctx = context(null);
        var request = describeGroupsRequest("orders-group");

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isTrue();
        var response = (DescribeGroupsResponseData) ctx.shortCircuitResult().body();
        assertThat(response.groups()).hasSize(1);
        assertThat(response.groups().iterator().next().errorCode())
                .isEqualTo(Errors.SASL_AUTHENTICATION_FAILED.code());
    }

    @Test
    void deniesDescribeGroupsWithEmptyResponseWhenBodyIsUndecoded() {
        var interceptor = new AuthorizationInterceptor(authorizer(),
                new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var request = new TestRequest(ApiKeys.DESCRIBE_GROUPS.id, (short) 99, null);

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isTrue();
        var response = (DescribeGroupsResponseData) ctx.shortCircuitResult().body();
        assertThat(response.groups()).isEmpty();
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
            return 5;
        }

        @Override
        public int correlationId() {
            return 42;
        }
    }
}

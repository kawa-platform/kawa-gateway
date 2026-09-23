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
import org.apache.kafka.common.message.ListGroupsRequestData;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ListGroupsAuthorizationCheckTest {

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

    private static Request listGroupsRequest() {
        return new TestRequest(ApiKeys.LIST_GROUPS.id, (short) 4, new ListGroupsRequestData());
    }

    private static Response listGroupsResponse(ListGroupsResponseData body) {
        return new TestResponse(ApiKeys.LIST_GROUPS.id, body);
    }

    private static ListGroupsResponseData brokerResponse(String... groups) {
        var body = new ListGroupsResponseData();
        for (String group : groups) {
            body.groups().add(new ListGroupsResponseData.ListedGroup().setGroupId(group));
        }
        return body;
    }

    @Test
    void keepsAuthorizedGroupAndRemovesDeniedGroup() {
        var interceptor = new AuthorizationInterceptor(
                authorizer(groupAcl("orders-group")),
                new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var request = listGroupsRequest();

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isFalse();

        var response = listGroupsResponse(brokerResponse("orders-group", "banned-group"));
        interceptor.onResponse(ctx, response);

        var responseBody = (ListGroupsResponseData) response.body();
        assertThat(responseBody.groups()).hasSize(1);
        assertThat(responseBody.groups().iterator().next().groupId()).isEqualTo("orders-group");
    }

    @Test
    void shortCircuitsWithAuthenticationFailedWhenThereIsNoPrincipal() {
        var interceptor = new AuthorizationInterceptor(authorizer(),
                new VirtualTopicManager(Map.of()));
        var ctx = context(null);
        var request = listGroupsRequest();

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isTrue();
        var response = (ListGroupsResponseData) ctx.shortCircuitResult().body();
        assertThat(response.errorCode()).isEqualTo(Errors.SASL_AUTHENTICATION_FAILED.code());
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

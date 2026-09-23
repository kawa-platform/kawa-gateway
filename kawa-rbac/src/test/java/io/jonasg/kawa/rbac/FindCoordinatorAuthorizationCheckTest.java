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
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class FindCoordinatorAuthorizationCheckTest {

    private static final byte COORDINATOR_TYPE_GROUP = 0;
    private static final byte COORDINATOR_TYPE_TRANSACTION = 1;

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

    private static Request findCoordinatorRequest(byte keyType, String key) {
        return new TestRequest(ApiKeys.FIND_COORDINATOR.id, (short) 2,
                new FindCoordinatorRequestData().setKeyType(keyType).setKey(key));
    }

    @Test
    void allowsGroupLookupWhenGroupDescribeIsAllowed() {
        var interceptor = new AuthorizationInterceptor(
                authorizer(acl(ResourceType.GROUP, "g1", AclOperation.DESCRIBE)),
                new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var request = findCoordinatorRequest(COORDINATOR_TYPE_GROUP, "g1");

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isFalse();
        var body = (FindCoordinatorRequestData) request.body();
        assertThat(body.key()).isEqualTo("g1");
        assertThat(body.keyType()).isEqualTo(COORDINATOR_TYPE_GROUP);
    }

    @Test
    void shortCircuitsGroupLookupWhenGroupDescribeIsDenied() {
        var interceptor = new AuthorizationInterceptor(authorizer(), new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var request = findCoordinatorRequest(COORDINATOR_TYPE_GROUP, "g1");

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isTrue();
        var result = ctx.shortCircuitResult();
        assertThat(result.body()).isInstanceOf(FindCoordinatorResponseData.class);
        var response = (FindCoordinatorResponseData) result.body();
        assertThat(response.errorCode()).isEqualTo(Errors.GROUP_AUTHORIZATION_FAILED.code());
    }

    @Test
    void allowsTransactionLookupWhenTransactionalIdDescribeIsAllowed() {
        var interceptor = new AuthorizationInterceptor(
                authorizer(acl(ResourceType.TRANSACTIONAL_ID, "txn-1", AclOperation.DESCRIBE)),
                new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var request = findCoordinatorRequest(COORDINATOR_TYPE_TRANSACTION, "txn-1");

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isFalse();
        var body = (FindCoordinatorRequestData) request.body();
        assertThat(body.key()).isEqualTo("txn-1");
        assertThat(body.keyType()).isEqualTo(COORDINATOR_TYPE_TRANSACTION);
    }

    @Test
    void shortCircuitsTransactionLookupWhenTransactionalIdDescribeIsDenied() {
        var interceptor = new AuthorizationInterceptor(authorizer(), new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var request = findCoordinatorRequest(COORDINATOR_TYPE_TRANSACTION, "txn-1");

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isTrue();
        var response = (FindCoordinatorResponseData) ctx.shortCircuitResult().body();
        assertThat(response.errorCode()).isEqualTo(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code());
    }

    @Test
    void forwardsUnrecognizedKeyTypeUnchanged() {
        var interceptor = new AuthorizationInterceptor(authorizer(), new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var request = findCoordinatorRequest((byte) 99, "whatever");

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isFalse();
    }

    @Test
    void deniesWithAuthenticationFailedWhenThereIsNoPrincipal() {
        var interceptor = new AuthorizationInterceptor(authorizer(), new VirtualTopicManager(Map.of()));
        var ctx = context(null);
        var request = findCoordinatorRequest(COORDINATOR_TYPE_GROUP, "g1");

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isTrue();
        var response = (FindCoordinatorResponseData) ctx.shortCircuitResult().body();
        assertThat(response.errorCode()).isEqualTo(Errors.SASL_AUTHENTICATION_FAILED.code());
    }

    @Test
    void deniesWithEmptyResponseWhenBodyIsUndecoded() {
        var interceptor = new AuthorizationInterceptor(authorizer(), new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var request = new TestRequest(ApiKeys.FIND_COORDINATOR.id, (short) 99, null);

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isTrue();
        var response = (FindCoordinatorResponseData) ctx.shortCircuitResult().body();
        assertThat(response.errorCode()).isEqualTo((short) 0);
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

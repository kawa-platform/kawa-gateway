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
import org.apache.kafka.common.message.DescribeTransactionsRequestData;
import org.apache.kafka.common.message.DescribeTransactionsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class DescribeTransactionsAuthorizationCheckTest {

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

    private static Request describeTransactionsRequest(String... transactionalIds) {
        var body = new DescribeTransactionsRequestData()
                .setTransactionalIds(new java.util.ArrayList<>(List.of(transactionalIds)));
        return new TestRequest(ApiKeys.DESCRIBE_TRANSACTIONS.id, (short) 0, body);
    }

    private static Response describeTransactionsResponse(DescribeTransactionsResponseData body) {
        return new TestResponse(ApiKeys.DESCRIBE_TRANSACTIONS.id, body);
    }

    private static DescribeTransactionsResponseData brokerResponse(String transactionalId) {
        var body = new DescribeTransactionsResponseData();
        body.transactionStates().add(new DescribeTransactionsResponseData.TransactionState()
                .setTransactionalId(transactionalId).setErrorCode((short) 0));
        return body;
    }

    @Test
    void allowsDescribeTransactionsWhenTransactionalIdDescribeIsAllowed() {
        var interceptor = new AuthorizationInterceptor(
                authorizer(acl(ResourceType.TRANSACTIONAL_ID, "txn-1", AclOperation.DESCRIBE)),
                new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var request = describeTransactionsRequest("txn-1");

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isFalse();
        var body = (DescribeTransactionsRequestData) request.body();
        assertThat(body.transactionalIds()).containsExactly("txn-1");
    }

    @Test
    void stripsDeniedTransactionalIdsAndMergesDenialIntoResponse() {
        var interceptor = new AuthorizationInterceptor(
                authorizer(acl(ResourceType.TRANSACTIONAL_ID, "txn-1", AclOperation.DESCRIBE)),
                new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var request = describeTransactionsRequest("txn-1", "txn-banned");

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isFalse();
        var body = (DescribeTransactionsRequestData) request.body();
        assertThat(body.transactionalIds()).containsExactly("txn-1");

        var response = describeTransactionsResponse(brokerResponse("txn-1"));
        interceptor.onResponse(ctx, response);

        var responseBody = (DescribeTransactionsResponseData) response.body();
        assertThat(responseBody.transactionStates()).hasSize(2);
        var banned = responseBody.transactionStates().stream()
                .filter(t -> t.transactionalId().equals("txn-banned")).findFirst().orElseThrow();
        assertThat(banned.errorCode()).isEqualTo(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code());
    }

    @Test
    void shortCircuitsDescribeTransactionsWhenEveryTransactionalIdIsDenied() {
        var interceptor = new AuthorizationInterceptor(
                authorizer(acl(ResourceType.TRANSACTIONAL_ID, "txn-1", AclOperation.DESCRIBE)),
                new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var request = describeTransactionsRequest("txn-banned");

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isTrue();
        var response = (DescribeTransactionsResponseData) ctx.shortCircuitResult().body();
        assertThat(response.transactionStates()).hasSize(1);
        assertThat(response.transactionStates().iterator().next().transactionalId()).isEqualTo("txn-banned");
        assertThat(response.transactionStates().iterator().next().errorCode())
                .isEqualTo(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code());
    }

    @Test
    void deniesDescribeTransactionsWithAuthenticationFailedWhenThereIsNoPrincipal() {
        var interceptor = new AuthorizationInterceptor(authorizer(), new VirtualTopicManager(Map.of()));
        var ctx = context(null);
        var request = describeTransactionsRequest("txn-1");

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isTrue();
        var response = (DescribeTransactionsResponseData) ctx.shortCircuitResult().body();
        assertThat(response.transactionStates()).hasSize(1);
        assertThat(response.transactionStates().iterator().next().errorCode())
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
            return 0;
        }

        @Override
        public int correlationId() {
            return 42;
        }
    }
}

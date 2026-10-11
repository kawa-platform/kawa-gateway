package io.jonasg.kawa.rbac;

import io.jonasg.kawa.config.AclConfig;
import io.jonasg.kawa.config.GroupConfig;
import io.jonasg.kawa.config.RbacConfig;
import io.jonasg.kawa.config.ResourceConfig;
import io.jonasg.kawa.config.RoleConfig;
import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.core.Request;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.message.AddOffsetsToTxnRequestData;
import org.apache.kafka.common.message.AddOffsetsToTxnResponseData;
import org.apache.kafka.common.message.EndTxnRequestData;
import org.apache.kafka.common.message.EndTxnResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/// RBAC for the transaction-ending APIs: AddOffsetsToTxn (TRANSACTIONAL_ID WRITE + GROUP READ) and
/// EndTxn (TRANSACTIONAL_ID WRITE).
class AddOffsetsToTxnAuthorizationCheckTest {

    private static AuthorizationInterceptor interceptor(AclConfig... acls) {
        return new AuthorizationInterceptor(new RbacAuthorizer(new RbacConfig(
                Map.of("producer", new RoleConfig(List.of(acls))),
                Map.of("producers", new GroupConfig(List.of("alice"), List.of("producer"))))),
                new VirtualTopicManager(Map.of()));
    }

    private static AclConfig transactionalIdWrite() {
        return new AclConfig(new ResourceConfig(ResourceType.TRANSACTIONAL_ID, "txn-orders"), AclOperation.WRITE);
    }

    private static AclConfig groupRead() {
        return new AclConfig(new ResourceConfig(ResourceType.GROUP, "orders-readers"), AclOperation.READ);
    }

    private static Request addOffsetsToTxn() {
        return new TestRequest(ApiKeys.ADD_OFFSETS_TO_TXN.id, (short) 3,
                new AddOffsetsToTxnRequestData().setTransactionalId("txn-orders").setGroupId("orders-readers"));
    }

    private static Request endTxn() {
        return new TestRequest(ApiKeys.END_TXN.id, (short) 3, new EndTxnRequestData().setTransactionalId("txn-orders"));
    }

    @Test
    void forwardsAddOffsetsToTxnWithTransactionalIdWriteAndGroupRead() {
        // given
        var context = new GatewayContext("source", 0L, "alice");

        // when
        interceptor(transactionalIdWrite(), groupRead()).onRequest(context, addOffsetsToTxn());

        // then
        assertThat(context.isShortCircuited())
                .withFailMessage(() -> "AddOffsetsToTxn was refused although 'alice' may write the transaction and read the group")
                .isFalse();
    }

    @Test
    void refusesAddOffsetsToTxnWithoutTransactionalIdWrite() {
        // given
        var context = new GatewayContext("source", 0L, "alice");

        // when
        interceptor(groupRead()).onRequest(context, addOffsetsToTxn());

        // then
        assertThat(errorOf(context))
                .withFailMessage(() -> "Expected TRANSACTIONAL_ID_AUTHORIZATION_FAILED, got error " + errorOf(context))
                .isEqualTo(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code());
    }

    @Test
    void refusesAddOffsetsToTxnWithoutGroupRead() {
        // given
        var context = new GatewayContext("source", 0L, "alice");

        // when
        interceptor(transactionalIdWrite()).onRequest(context, addOffsetsToTxn());

        // then
        assertThat(errorOf(context))
                .withFailMessage(() -> "Expected GROUP_AUTHORIZATION_FAILED, got error " + errorOf(context))
                .isEqualTo(Errors.GROUP_AUTHORIZATION_FAILED.code());
    }

    @Test
    void refusesAddOffsetsToTxnWithoutAPrincipal() {
        // given
        var context = new GatewayContext("source", 0L, null);

        // when
        interceptor(transactionalIdWrite(), groupRead()).onRequest(context, addOffsetsToTxn());

        // then
        assertThat(errorOf(context))
                .withFailMessage(() -> "Expected SASL_AUTHENTICATION_FAILED, got error " + errorOf(context))
                .isEqualTo(Errors.SASL_AUTHENTICATION_FAILED.code());
    }

    @Test
    void forwardsEndTxnWithTransactionalIdWrite() {
        // given
        var context = new GatewayContext("source", 0L, "alice");

        // when
        interceptor(transactionalIdWrite()).onRequest(context, endTxn());

        // then
        assertThat(context.isShortCircuited())
                .withFailMessage(() -> "EndTxn was refused although 'alice' may write the transaction")
                .isFalse();
    }

    @Test
    void refusesEndTxnWithoutTransactionalIdWrite() {
        // given
        var context = new GatewayContext("source", 0L, "alice");

        // when
        interceptor().onRequest(context, endTxn());

        // then
        var response = (EndTxnResponseData) context.shortCircuitResult().body();
        assertThat(response.errorCode())
                .withFailMessage(() -> "Expected TRANSACTIONAL_ID_AUTHORIZATION_FAILED, got error " + response.errorCode())
                .isEqualTo(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code());
    }

    private static short errorOf(GatewayContext context) {
        assertThat(context.isShortCircuited())
                .withFailMessage(() -> "The request was forwarded instead of refused")
                .isTrue();
        return ((AddOffsetsToTxnResponseData) context.shortCircuitResult().body()).errorCode();
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
}

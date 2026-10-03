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
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.message.InitProducerIdResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/// InitProducerId and ConsumerGroupHeartbeat, decoded since governance judges them.
class InitProducerIdAuthorizationCheckTest {

    private static AuthorizationInterceptor interceptor(AclConfig... acls) {
        return new AuthorizationInterceptor(new RbacAuthorizer(new RbacConfig(
                Map.of("role", new RoleConfig(List.of(acls))),
                Map.of("group", new GroupConfig(List.of("alice"), List.of("role"))))),
                new VirtualTopicManager(Map.of()));
    }

    private static AclConfig acl(ResourceType type, String name, AclOperation operation) {
        return new AclConfig(new ResourceConfig(type, name), operation);
    }

    private static Request initProducerId(String transactionalId) {
        return new TestRequest(ApiKeys.INIT_PRODUCER_ID.id, (short) 4, new InitProducerIdRequestData().setTransactionalId(transactionalId));
    }

    @Test
    void allowsATransactionalProducerWithTransactionalIdWrite() {
        // given
        var context = new GatewayContext("source", 0L, "alice");

        // when
        interceptor(acl(ResourceType.TRANSACTIONAL_ID, "tx-1", AclOperation.WRITE)).onRequest(context, initProducerId("tx-1"));

        // then
        assertThat(context.isShortCircuited()).isFalse();
    }

    @Test
    void refusesATransactionalProducerWithoutTransactionalIdWrite() {
        // given
        var context = new GatewayContext("source", 0L, "alice");

        // when
        interceptor().onRequest(context, initProducerId("tx-1"));

        // then
        var body = (InitProducerIdResponseData) context.shortCircuitResult().body();
        assertThat(body.errorCode()).isEqualTo(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code());
    }

    @Test
    void forwardsAnIdempotentProducer() {
        // given
        var context = new GatewayContext("source", 0L, "alice");

        // when
        interceptor().onRequest(context, initProducerId(null));

        // then
        assertThat(context.isShortCircuited())
                .withFailMessage(() -> "An idempotent producer was refused for lacking transactional id permissions")
                .isFalse();
    }

    @Test
    void refusesAnUnauthenticatedProducer() {
        // given
        var context = new GatewayContext("source", 0L, null);

        // when
        interceptor().onRequest(context, initProducerId(null));

        // then
        var body = (InitProducerIdResponseData) context.shortCircuitResult().body();
        assertThat(body.errorCode()).isEqualTo(Errors.SASL_AUTHENTICATION_FAILED.code());
    }

    @Test
    void gatesConsumerGroupHeartbeatOnGroupRead() {
        // given
        var context = new GatewayContext("source", 0L, "alice");
        var request = new TestRequest(ApiKeys.CONSUMER_GROUP_HEARTBEAT.id, (short) 0,
                new ConsumerGroupHeartbeatRequestData().setGroupId("g1"));

        // when
        interceptor().onRequest(context, request);

        // then
        var body = (ConsumerGroupHeartbeatResponseData) context.shortCircuitResult().body();
        assertThat(body.errorCode()).isEqualTo(Errors.GROUP_AUTHORIZATION_FAILED.code());
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

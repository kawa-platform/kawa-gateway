package io.jonasg.kawa.rbac;

import io.jonasg.kawa.config.RbacConfig;
import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.core.Request;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import org.apache.kafka.common.message.DescribeClusterRequestData;
import org.apache.kafka.common.message.DescribeClusterResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class DescribeClusterAuthorizationCheckTest {

    // no roles at all: describing the cluster needs no ACL, only an authenticated principal
    private final AuthorizationInterceptor interceptor = new AuthorizationInterceptor(
            new RbacAuthorizer(new RbacConfig(Map.of(), Map.of())), new VirtualTopicManager(Map.of()));

    @Test
    void forwardsDescribeClusterForAnAuthenticatedPrincipal() {
        // given
        var context = new GatewayContext("source", 0L, "alice");

        // when
        interceptor.onRequest(context, describeClusterRequest());

        // then
        assertThat(context.isShortCircuited())
                .withFailMessage(() -> "DescribeCluster was refused for authenticated principal 'alice'")
                .isFalse();
    }

    @Test
    void deniesDescribeClusterWithAuthenticationFailedWhenThereIsNoPrincipal() {
        // given
        var context = new GatewayContext("source", 0L, null);

        // when
        interceptor.onRequest(context, describeClusterRequest());

        // then
        assertThat(context.isShortCircuited())
                .withFailMessage(() -> "DescribeCluster without an authenticated principal was forwarded")
                .isTrue();
        var response = (DescribeClusterResponseData) context.shortCircuitResult().body();
        assertThat(response.errorCode())
                .withFailMessage(() -> "Expected SASL_AUTHENTICATION_FAILED, got error " + response.errorCode())
                .isEqualTo(Errors.SASL_AUTHENTICATION_FAILED.code());
    }

    private static Request describeClusterRequest() {
        return new TestRequest(ApiKeys.DESCRIBE_CLUSTER.id, (short) 1, new DescribeClusterRequestData());
    }

    private record TestRequest(int apiKey, short apiVersion, Object body) implements Request {
        @Override
        public String apiName() {
            return "DescribeCluster";
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

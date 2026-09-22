package io.jonasg.kawa.server;

import io.jonasg.kawa.config.AclConfig;
import io.jonasg.kawa.config.AuthConfig;
import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.GovernanceConfig;
import io.jonasg.kawa.config.GovernanceRuleConfig;
import io.jonasg.kawa.config.GroupConfig;
import io.jonasg.kawa.config.RbacConfig;
import io.jonasg.kawa.config.ResourceConfig;
import io.jonasg.kawa.config.RoleConfig;
import io.jonasg.kawa.config.ClientConfig;
import io.jonasg.kawa.config.HashedPassword;
import io.jonasg.kawa.config.Mechanism;
import io.jonasg.kawa.config.VirtualTopicConfig;
import io.jonasg.kawa.governance.TopicSpec;
import io.jonasg.kawa.server.auth.AuthenticationResult;
import io.jonasg.kawa.server.netty.ClientSession;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class DynamicGatewayStateTest {

    @Test
    void applyReachesAllConsumers() {
        // given
        var state = new DynamicGatewayState("localhost:9092", "__kawa", null);
        var config = GatewayConfig.empty()
                .upsertVirtualTopic("orders", new VirtualTopicConfig("orders-physical"))
                .updateRbac(new RbacConfig(
                        Map.of("reader", new RoleConfig(List.of(new AclConfig(
                                new ResourceConfig(ResourceType.TOPIC, "orders-physical"),
                                AclOperation.READ)))),
                        Map.of("readers", new GroupConfig(List.of("alice"), List.of("reader")))))
                .updateAuth(new AuthConfig(Set.of("PLAIN"),
                        Map.of("alice", new ClientConfig("PLAIN",
                                HashedPassword.fromPlaintext(Mechanism.PLAIN, "secret"))), null))
                .updateGovernance(new GovernanceConfig(
                        Map.of("no-delete", new GovernanceRuleConfig(
                                "must not delete", "topic.name != 'deleted'")), Map.of()));

        // when
        state.configManager().apply(config);

        // then
        assertThat(state.virtualTopics().virtualTopics()).containsEntry("orders", "orders-physical");
        assertThat(state.authorizer().hasAnyAcls()).isTrue();
        assertThat(state.authorizer().isAuthorized(
                "alice", ResourceType.TOPIC, "orders-physical", AclOperation.READ)).isTrue();
        var session = new ClientSession(new EmbeddedChannel());
        assertThat(state.saslAuthenticator().handleHandshake(session,
                new SaslHandshakeRequestData().setMechanism("PLAIN")).errorCode()).isEqualTo(Errors.NONE.code());
        assertThat(state.saslAuthenticator().handleAuthenticate(session,
                new SaslAuthenticateRequestData()
                        .setAuthBytes("\u0000alice\u0000secret".getBytes(StandardCharsets.UTF_8))))
                .isInstanceOf(AuthenticationResult.Success.class);
        assertThat(state.governance().evaluate("alice", "svc",
                new TopicSpec("deleted", 1, 1, Map.of()))).isNotEmpty();

        state.close();
    }
}

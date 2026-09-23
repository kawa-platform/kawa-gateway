package io.jonasg.kawa.server;

import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import io.jonasg.kawa.protocol.kafka.KafkaApiRegistry;
import io.jonasg.kawa.protocol.kafka.KafkaApiSpec;
import io.jonasg.kawa.rbac.AuthorizationInterceptor;
import io.jonasg.kawa.rbac.RbacAuthorizer;
import io.jonasg.kawa.config.RbacConfig;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/// Guards the RBAC surface against drift between the protocol registry (what kawa decodes and
/// advertises) and the authorization interceptor (what kawa gates). A registered-but-ungated API
/// is a privilege-escalation hole; a check for an unregistered API never sees a decoded body.
class ApiRegisterCoverageTest {

    private static final Set<Short> INTENTIONALLY_UNGATED = Set.of(
            ApiKeys.API_VERSIONS.id,        // pre-auth version negotiation
            ApiKeys.SASL_HANDSHAKE.id,      // the authentication handshake itself
            ApiKeys.SASL_AUTHENTICATE.id);  // gating these would be circular

    private static AuthorizationInterceptor interceptor() {
        return new AuthorizationInterceptor(
                new RbacAuthorizer(new RbacConfig(Map.of(), Map.of())),
                new VirtualTopicManager(Map.of()));
    }

    @Test
    void everyRegisteredApiIsEitherGatedOrExplicitlyExempt() {
        var gated = interceptor().gatedApiKeys();

        var ungated = KafkaApiRegistry.create().specs().stream()
                .map(KafkaApiSpec::apiKey)
                .filter(apiKey -> !gated.contains(apiKey))
                .filter(apiKey -> !INTENTIONALLY_UNGATED.contains(apiKey))
                .map(apiKey -> ApiKeys.forId(apiKey).name())
                .toList();

        assertThat(ungated)
                .as("every decoded and advertised API must be authorized, or listed in "
                    + "INTENTIONALLY_UNGATED with a comment saying why")
                .isEmpty();
    }

    @Test
    void everyAuthorizationCheckGatesARegisteredApi() {
        var registered = KafkaApiRegistry.create().specs().stream()
                .map(KafkaApiSpec::apiKey)
                .collect(Collectors.toSet());

        var dead = interceptor().gatedApiKeys().stream()
                .filter(apiKey -> !registered.contains(apiKey))
                .map(apiKey -> ApiKeys.forId(apiKey).name())
                .toList();

        assertThat(dead)
                .as("a check for an unregistered API never sees a decoded body - it either denies "
                    + "everything or silently does nothing")
                .isEmpty();
    }
}

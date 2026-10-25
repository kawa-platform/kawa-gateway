package io.jonasg.kawa.it;

import io.jonasg.kawa.config.AuthConfig;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.AuthorizationException;
import org.junit.jupiter.api.Test;

import java.net.http.HttpRequest;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static io.jonasg.kawa.test.KawaAssertions.assertThat;
import static io.jonasg.kawa.test.KawaAssertions.assertThatThrownBy;

/// The RBAC module switched off and on at runtime through the admin API. RBAC is fail-open: while
/// it is off, a principal without a single role is admitted; once it is back on, default-deny
/// applies again.
class RbacModuleIT extends AdminHTTPTestSupport {

    private static final String VISITOR = "visitor";
    private static final String VISITOR_PASSWORD = "visitor-secret";

    @Override
    protected AuthConfig authConfig() {
        // the visitor can log in, but no role grants it anything
        return new AuthConfig(
                Set.of("PLAIN"),
                Map.of(DEFAULT_PRINCIPAL, client("PLAIN", DEFAULT_PASSWORD), VISITOR, client("PLAIN", VISITOR_PASSWORD)));
    }

    @Test
    void admitsAPrincipalWithoutRolesWhileRbacIsSwitchedOff() throws Exception {
        try (var visitor = visitorAdmin()) {
            // given
            assertDenied(visitor, "visitor.before");

            // when
            switchRbac(false);

            // then
            visitor.createTopics(List.of(new NewTopic("visitor.while-off", 1, (short) 1))).all().get(10, TimeUnit.SECONDS);
            eventually(() -> assertThat(brokerTopics())
                    .withFailMessage(() -> "'visitor' could not create a topic while RBAC was off")
                    .contains("visitor.while-off"));

            // and when
            switchRbac(true);

            // then
            assertDenied(visitor, "visitor.after");
        }
    }

    private void assertDenied(AdminClient visitor, String topic) throws Exception {
        assertThatThrownBy(() -> visitor.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(10, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(AuthorizationException.class);
        assertThat(brokerTopics())
                .withFailMessage(() -> "'visitor' created '" + topic + "' without any role while RBAC was on")
                .doesNotContain(topic);
    }

    private static AdminClient visitorAdmin() {
        var props = saslProps(gatewayBootstrap);
        props.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.plain.PlainLoginModule required "
                + "username=\"" + VISITOR + "\" password=\"" + VISITOR_PASSWORD + "\";");
        return AdminClient.create(props);
    }

    private Set<String> brokerTopics() throws Exception {
        return brokerAdmin.listTopics().names().get(10, TimeUnit.SECONDS);
    }

    private void switchRbac(boolean enabled) throws Exception {
        var response = httpExec(reqBuilder("/modules/rbac?consistency=applied")
                .header("Content-Type", "application/json")
                .PUT(HttpRequest.BodyPublishers.ofString("{\"enabled\":" + enabled + "}"))
                .build());
        assertThat(response).hasStatusCode(200);
    }
}

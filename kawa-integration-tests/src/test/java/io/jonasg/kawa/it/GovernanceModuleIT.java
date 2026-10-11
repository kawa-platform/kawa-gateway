package io.jonasg.kawa.it;

import io.jonasg.kawa.config.GovernanceConfig;
import io.jonasg.kawa.config.GovernanceRuleConfig;
import io.jonasg.kawa.config.GovernanceRuleConfig.Expression;
import io.jonasg.kawa.config.GovernanceRuleConfig.Selector;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.junit.jupiter.api.Test;

import java.net.http.HttpRequest;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static io.jonasg.kawa.test.KawaAssertions.assertThat;
import static io.jonasg.kawa.test.KawaAssertions.assertThatThrownBy;

/// The governance module switched off and on at runtime through the admin API: while it is off no
/// governance rule is applied to Kafka requests, and its rules apply again once it is back on.
class GovernanceModuleIT extends AdminHTTPTestSupport {

    @Override
    protected GovernanceConfig governanceConfig() {
        return new GovernanceConfig(Map.of("naming", new GovernanceRuleConfig("naming",
                "Topic names start with 'team.'.", null, Selector.topic(),
                Expression.cel("topic.name.startsWith('team.')"))));
    }

    @Test
    void appliesNoGovernanceWhileGovernanceIsSwitchedOff() throws Exception {
        // given
        assertRefused("misc.before");

        // when
        switchGovernance(false);

        // then
        gatewayAdmin.createTopics(List.of(new NewTopic("misc.while-off", 1, (short) 1))).all().get(10, TimeUnit.SECONDS);
        assertThat(brokerTopics())
                .withFailMessage(() -> "Non-compliant topic 'misc.while-off' was not created while governance was off")
                .contains("misc.while-off");

        // and when
        switchGovernance(true);

        // then
        assertRefused("misc.after");
    }

    private void assertRefused(String topic) throws Exception {
        assertThatThrownBy(() -> gatewayAdmin.createTopics(List.of(new NewTopic(topic, 1, (short) 1)))
                .all().get(10, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(PolicyViolationException.class);
        assertThat(brokerTopics())
                .withFailMessage(() -> "Non-compliant topic '" + topic + "' reached the broker while governance was on")
                .doesNotContain(topic);
    }

    private Set<String> brokerTopics() throws Exception {
        return brokerAdmin.listTopics().names().get(10, TimeUnit.SECONDS);
    }

    private void switchGovernance(boolean enabled) throws Exception {
        var response = httpExec(reqBuilder("/modules/governance?consistency=applied")
                .header("Content-Type", "application/json")
                .PUT(HttpRequest.BodyPublishers.ofString("{\"enabled\":" + enabled + "}"))
                .build());
        assertThat(response).hasStatusCode(200);
    }
}

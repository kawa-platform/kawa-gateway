package io.jonasg.kawa.it;

import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Test;

import java.net.http.HttpRequest;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static io.jonasg.kawa.test.KawaAssertions.assertThat;

/// The virtual-topics module switched off and on at runtime through the admin API: while it is off
/// clients see the cluster's physical topics, but still reach every broker through the gateway.
class VirtualTopicsModuleIT extends AdminHTTPTestSupport {

    @Override
    protected Map<String, String> virtualTopics() {
        return Map.of("orders", "orders-v2");
    }

    @Override
    protected List<NewTopic> initialTopics() {
        return List.of(new NewTopic("orders-v2", 1, (short) 1));
    }

    @Test
    void showsPhysicalTopicsWhileVirtualTopicsAreSwitchedOff() throws Exception {
        // given
        assertThat(topicNames())
                .withFailMessage(() -> "virtual topic 'orders' is not listed while the module is on")
                .contains("orders")
                .doesNotContain("orders-v2");

        // when
        switchVirtualTopics(false);

        // then
        Set<String> whileOff = topicNames();
        assertThat(whileOff)
                .withFailMessage(() -> "expected the physical topic 'orders-v2' only while the module is off: " + whileOff)
                .contains("orders-v2")
                .doesNotContain("orders");
        int leaderPort = gatewayAdmin.describeTopics(List.of("orders-v2")).allTopicNames().get(30, TimeUnit.SECONDS)
                .get("orders-v2").partitions().getFirst().leader().port();
        assertThat(leaderPort)
                .withFailMessage(() -> "the leader of 'orders-v2' points at port " + leaderPort
                        + " instead of the gateway's " + gateway.boundPort() + " while virtual topics are off")
                .isEqualTo(gateway.boundPort());

        // and when
        switchVirtualTopics(true);

        // then
        Set<String> whileOn = topicNames();
        assertThat(whileOn)
                .withFailMessage(() -> "virtual topic 'orders' is not listed again after switching the module back on: " + whileOn)
                .contains("orders")
                .doesNotContain("orders-v2");
    }

    private Set<String> topicNames() throws Exception {
        return gatewayAdmin.listTopics().names().get(30, TimeUnit.SECONDS);
    }

    private void switchVirtualTopics(boolean enabled) throws Exception {
        var response = httpExec(reqBuilder("/modules/virtual-topics?consistency=applied")
                .header("Content-Type", "application/json")
                .PUT(HttpRequest.BodyPublishers.ofString("{\"enabled\":" + enabled + "}"))
                .build());
        assertThat(response).hasStatusCode(200);
    }
}

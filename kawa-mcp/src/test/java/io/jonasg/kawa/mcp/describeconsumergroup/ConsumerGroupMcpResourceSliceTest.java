package io.jonasg.kawa.mcp.describeconsumergroup;

import io.jonasg.kawa.broker.BrokerAdmin.GroupDescription;
import io.jonasg.kawa.broker.BrokerAdmin.GroupMember;
import io.jonasg.kawa.broker.BrokerAdmin.PartitionId;
import io.jonasg.kawa.mcp.McpSliceTestBase;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.jonasg.kawa.test.KawaAssertions.assertThat;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;

class ConsumerGroupMcpResourceSliceTest extends McpSliceTestBase {

    @Test
    void readsOneGroupWithItsMembers() throws Exception {
        // given
        brokerAdmin.groups.put("orders-consumers", new GroupDescription("orders-consumers", "CLASSIC", "Stable", List.of(
                new GroupMember("member-1", "client-1", "/10.0.0.1", List.of(new PartitionId("orders-v2", 0))))));
        startServer();

        // when
        var readResp = readResource("kafka://consumer-groups/orders-consumers");

        // then
        assertThat(readResp).withFailMessage(() -> "resources/read of orders-consumers was not answered with 200")
                .hasStatusCode(200);
        assertThatJson(readResp.body()).inPath("result.contents[0].text").isString()
                .satisfies(text -> assertThatJson(text).inPath("state")
                        .withFailMessage(() -> "orders-consumers was not described with its state: " + readResp.body())
                        .isEqualTo("Stable"));
    }

    @Test
    void answersResourceNotFoundForAGroupTheBrokerDoesNotKnow() throws Exception {
        // given
        startServer();

        // when
        var readResp = readResource("kafka://consumer-groups/missing");

        // then
        assertThatJson(readResp.body()).inPath("error.code")
                .withFailMessage(() -> "an unknown group was not refused as a resource that does not exist: " + readResp.body())
                .isEqualTo(-32002);
    }
}

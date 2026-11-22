package io.jonasg.kawa.mcp.listconsumergroups;

import io.jonasg.kawa.broker.BrokerAdmin.GroupDescription;
import io.jonasg.kawa.mcp.McpSliceTestBase;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.jonasg.kawa.test.KawaAssertions.assertThat;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;

class ConsumerGroupsMcpResourceSliceTest extends McpSliceTestBase {

    @Test
    void listsEveryGroupOnTheClusterAsTheListToolDoes() throws Exception {
        // given
        brokerAdmin.groups.put("orders-consumers", new GroupDescription("orders-consumers", "CLASSIC", "Empty", List.of()));
        startServer();

        // when
        var readResp = readResource("kafka://consumer-groups");

        // then
        assertThat(readResp).withFailMessage(() -> "resources/read of kafka://consumer-groups was not answered with 200")
                .hasStatusCode(200);
        assertThatJson(readResp.body()).inPath("result.contents[0].text").isString()
                .satisfies(text -> assertThatJson(text).inPath("groups[*].groupId")
                        .withFailMessage(() -> "kafka://consumer-groups did not list 'orders-consumers': " + readResp.body())
                        .isEqualTo(List.of("orders-consumers")));
    }
}

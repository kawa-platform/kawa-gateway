package io.jonasg.kawa.mcp;

import org.junit.jupiter.api.Test;

import java.util.List;

import static io.jonasg.kawa.test.KawaAssertions.assertThat;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;

class McpResourcesSliceTest extends McpSliceTestBase {

    @Test
    void listsTheTopicAndConsumerGroupResourcesAndTheirTemplates() throws Exception {
        // given
        startServer();

        // when
        var listResp = rpc("resources/list", "{}");
        var templatesResp = rpc("resources/templates/list", "{}");

        // then
        assertThat(listResp).withFailMessage(() -> "resources/list was not answered with 200").hasStatusCode(200);
        assertThatJson(listResp.body()).inPath("result.resources[*].uri")
                .withFailMessage(() -> "resources/list does not advertise the topics and consumer groups: " + listResp.body())
                .isEqualTo(List.of("kawa://topics", "kawa://consumer-groups"));
        assertThatJson(templatesResp.body()).inPath("result.resourceTemplates[*].uriTemplate").isArray()
                .withFailMessage(() -> "resources/templates/list does not advertise the single-item templates: " + templatesResp.body())
                .containsExactlyInAnyOrder("kawa://topics/{topicName}", "kawa://consumer-groups/{groupId}");
    }
}

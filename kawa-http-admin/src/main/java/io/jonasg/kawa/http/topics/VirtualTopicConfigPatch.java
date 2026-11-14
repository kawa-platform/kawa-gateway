package io.jonasg.kawa.http.topics;

import io.jonasg.kawa.config.PayloadFormatConfig;
import io.jonasg.kawa.config.VirtualTopicFilterConfig;
import tools.jackson.databind.JsonNode;

import java.util.List;

/// A partial update for an existing virtual topic. Missing fields keep their current values;
/// an omitted or null `filter` clears the current filter, and likewise `valueFormat` - the two
/// describe one content filter, so they are replaced together. An omitted or null `transformations`
/// keeps the current steps, `[]` removes them; they stay raw JSON for [TopicTransformations].
record VirtualTopicConfigPatch(
        String name,
        String topic,
        VirtualTopicFilterConfig filter,
        Boolean exposePhysicalTopic,
        PayloadFormatConfig valueFormat,
        List<JsonNode> transformations
) {
}

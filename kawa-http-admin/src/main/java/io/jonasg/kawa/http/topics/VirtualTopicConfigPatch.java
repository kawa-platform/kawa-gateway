package io.jonasg.kawa.http.topics;

import io.jonasg.kawa.config.PayloadFormatConfig;
import io.jonasg.kawa.config.VirtualTopicFilterConfig;
import org.jspecify.annotations.Nullable;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.util.List;

/// A partial update for an existing virtual topic. Missing fields keep their current values.
/// An explicit `null` `filter` or `valueFormat` clears it; both stay raw JSON so an omitted field
/// (Java `null`) can be told apart from an explicit `null` (a `NullNode`). An omitted or `null`
/// `transformations` keeps the current steps, `[]` removes them; they stay raw JSON for
/// [TopicTransformations].
record VirtualTopicConfigPatch(
        String name,
        String topic,
        @Nullable JsonNode filter,
        Boolean exposePhysicalTopic,
        @Nullable JsonNode valueFormat,
        List<JsonNode> transformations
) {

    private static final JsonMapper MAPPER = JsonMapper.builder().build();

    /// The filter after this patch: `current` when omitted, none when `null`, otherwise the given one.
    @Nullable VirtualTopicFilterConfig filterOr(@Nullable VirtualTopicFilterConfig current) {
        return patched(filter, current, VirtualTopicFilterConfig.class);
    }

    /// The value format after this patch: `current` when omitted, none when `null`, otherwise the given one.
    @Nullable PayloadFormatConfig valueFormatOr(@Nullable PayloadFormatConfig current) {
        return patched(valueFormat, current, PayloadFormatConfig.class);
    }

    private static <T> @Nullable T patched(@Nullable JsonNode field, @Nullable T current, Class<T> type) {
        if (field == null) {
            return current;
        }
        if (field.isNull()) {
            return null;
        }
        try {
            return MAPPER.treeToValue(field, type);
        } catch (JacksonException e) {
            throw new IllegalArgumentException("invalid topic body: " + e.getOriginalMessage(), e);
        }
    }
}

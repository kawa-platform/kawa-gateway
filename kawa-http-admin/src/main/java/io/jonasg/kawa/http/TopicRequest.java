package io.jonasg.kawa.http;

import io.jonasg.kawa.config.PayloadFormatConfig;
import io.jonasg.kawa.config.VirtualTopicFilterConfig;
import org.jspecify.annotations.NullUnmarked;

import java.util.Map;

/// The `POST /topics` and `PUT /topics/{name}` request body: a `type` discriminator
/// plus the fields of one topic kind. Physical topics carry partitions/replicationFactor/configs;
/// virtual topics carry the physical backing topic, optional filter and optional (record) value
/// format. Named `TopicRequest` rather than `TopicConfigRequest` because `type` names the route
/// resource (`physical` or `virtual`), not a config record.
@NullUnmarked
public record TopicRequest(
        String type,
        String name,
        Integer partitions,
        Short replicationFactor,
        Map<String, String> configs,
        String topic,
        VirtualTopicFilterConfig filter,
        Boolean exposePhysicalTopic,
        PayloadFormatConfig valueFormat
) {

    public TopicRequest {
        configs = configs == null ? Map.of() : Map.copyOf(configs);
    }
}

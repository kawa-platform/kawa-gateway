package io.jonasg.kawa.http;

import io.jonasg.kawa.config.PayloadFormatConfig;
import io.jonasg.kawa.config.VirtualTopicFilterConfig;

import java.util.Map;

/// The `POST /topics` request body: a `type` discriminator plus the fields of one
/// topic kind. Physical topics carry partitions/replicationFactor/configs; virtual topics
/// carry the physical backing topic, optional filter and optional (record) value format.
public record TopicCreateRequest(
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

    public TopicCreateRequest {
        configs = configs == null ? Map.of() : Map.copyOf(configs);
    }
}

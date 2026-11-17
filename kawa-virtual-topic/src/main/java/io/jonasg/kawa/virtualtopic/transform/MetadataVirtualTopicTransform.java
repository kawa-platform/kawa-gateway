package io.jonasg.kawa.virtualtopic.transform;

import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import io.jonasg.kawa.virtualtopic.VirtualTopicState;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class MetadataVirtualTopicTransform
        implements VirtualTopicTransform<MetadataRequestData, MetadataResponseData> {

    private final VirtualTopicManager virtualTopics;

    public MetadataVirtualTopicTransform(VirtualTopicManager virtualTopics) {
        this.virtualTopics = virtualTopics;
    }

    @Override
    public short apiKey() {
        return 3;
    }

    @Override
    public void onRequest(
            GatewayContext context,
            MetadataRequestData data
    ) {
        if (data.topics() == null) {
            return;
        }
        Set<String> requested = new HashSet<>();
        for (MetadataRequestData.MetadataRequestTopic topic : data.topics()) {
            requested.add(topic.name());
            topic.setName(virtualTopics.toPhysical(topic.name()));
        }
        VirtualTopicState.from(context).requestedTopics(requested);
    }

    @Override
    public void onResponse(
            GatewayContext context,
            MetadataResponseData data
    ) {
        // A physical topic is listed once per virtual topic the principal may see (and the request named),
        // and under its own name only with exposePhysicalTopic. Iterate a copy, since this adds to data.topics().
        VirtualTopicState state = VirtualTopicState.from(context);
        for (MetadataResponseData.MetadataResponseTopic topic : new ArrayList<>(data.topics())) {
            if (!virtualTopics.hasVirtualTopic(topic.name())) {
                continue;
            }
            List<String> visible = virtualTopics.toVirtual(topic.name()).stream()
                    .filter(virtual -> !state.isHidden(virtual))
                    .toList();
            if (visible.isEmpty()) {
                // Authorization removes an entry whose virtual topics are all hidden; never list it under none.
                data.topics().remove(topic);
                continue;
            }
            List<String> requested = visible.stream().filter(state::wasRequested).toList();
            // A request naming only the physical topic named none of its virtual topics: answer it under all of them.
            List<String> virtuals = requested.isEmpty() ? visible : requested;
            boolean exposed = virtuals.stream().anyMatch(virtualTopics::exposesPhysicalTopic);
            List<String> added = exposed ? virtuals : virtuals.subList(1, virtuals.size());
            for (String name : added) {
                data.topics().add(new MetadataResponseData.MetadataResponseTopic()
                        .setName(name)
                        .setTopicId(topic.topicId())
                        .setIsInternal(topic.isInternal())
                        .setPartitions(topic.partitions()));
            }
            if (!exposed) {
                topic.setName(virtuals.getFirst());
            }
        }
    }
}

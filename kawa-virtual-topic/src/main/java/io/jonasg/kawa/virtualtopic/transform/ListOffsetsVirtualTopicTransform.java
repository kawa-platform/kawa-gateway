package io.jonasg.kawa.virtualtopic.transform;

import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import io.jonasg.kawa.virtualtopic.VirtualTopicState;
import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.message.ListOffsetsResponseData;

import java.util.Objects;

public final class ListOffsetsVirtualTopicTransform
        implements VirtualTopicTransform<ListOffsetsRequestData, ListOffsetsResponseData> {

    private final VirtualTopicManager virtualTopics;

    public ListOffsetsVirtualTopicTransform(VirtualTopicManager virtualTopics) {
        this.virtualTopics = virtualTopics;
    }

    @Override
    public short apiKey() {
        return 2;
    }

    @Override
    public void onRequest(
            GatewayContext context,
            ListOffsetsRequestData data
    ) {
        var state = VirtualTopicState.from(context);
        for (ListOffsetsRequestData.ListOffsetsTopic topic : data.topics()) {
            String physical = virtualTopics.toPhysical(topic.name());
            if (!Objects.equals(physical, topic.name())) {
                state.record(physical, topic.name());
                topic.setName(physical);
            }
        }
    }

    @Override
    public void onResponse(
            GatewayContext context,
            ListOffsetsResponseData data
    ) {
        var state = VirtualTopicState.from(context);
        for (ListOffsetsResponseData.ListOffsetsTopicResponse topic : data.topics()) {
            String virtual = state.virtualFor(topic.name());
            if (virtual != null) {
                topic.setName(virtual);
            }
        }
    }
}

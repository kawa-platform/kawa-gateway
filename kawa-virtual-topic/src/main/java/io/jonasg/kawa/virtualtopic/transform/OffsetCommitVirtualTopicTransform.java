package io.jonasg.kawa.virtualtopic.transform;

import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import io.jonasg.kawa.virtualtopic.VirtualTopicState;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;

import java.util.Objects;

public final class OffsetCommitVirtualTopicTransform
        implements VirtualTopicTransform<OffsetCommitRequestData, OffsetCommitResponseData> {

    private final VirtualTopicManager virtualTopics;

    public OffsetCommitVirtualTopicTransform(VirtualTopicManager virtualTopics) {
        this.virtualTopics = virtualTopics;
    }

    @Override
    public short apiKey() {
        return 8;
    }

    @Override
    public void onRequest(
            GatewayContext context,
            OffsetCommitRequestData data
    ) {
        VirtualTopicState state = VirtualTopicState.from(context);
        for (OffsetCommitRequestData.OffsetCommitRequestTopic topic : data.topics()) {
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
            OffsetCommitResponseData data
    ) {
        VirtualTopicState state = VirtualTopicState.from(context);
        for (OffsetCommitResponseData.OffsetCommitResponseTopic topic : data.topics()) {
            String virtual = state.virtualFor(topic.name());
            if (virtual != null) {
                topic.setName(virtual);
            }
        }
    }
}

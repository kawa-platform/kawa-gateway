package io.jonasg.kawa.virtualtopic.transform;

import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import io.jonasg.kawa.virtualtopic.VirtualTopicState;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData;

import java.util.Objects;

public final class OffsetForLeaderEpochVirtualTopicTransform
        implements VirtualTopicTransform<OffsetForLeaderEpochRequestData, OffsetForLeaderEpochResponseData> {

    private static final short OFFSET_FOR_LEADER_EPOCH = 23;

    private final VirtualTopicManager virtualTopics;

    public OffsetForLeaderEpochVirtualTopicTransform(VirtualTopicManager virtualTopics) {
        this.virtualTopics = virtualTopics;
    }

    @Override
    public short apiKey() {
        return OFFSET_FOR_LEADER_EPOCH;
    }

    @Override
    public void onRequest(
            GatewayContext context,
            OffsetForLeaderEpochRequestData data
    ) {
        VirtualTopicState state = VirtualTopicState.from(context);
        for (OffsetForLeaderEpochRequestData.OffsetForLeaderTopic topic : data.topics()) {
            String virtual = topic.topic();
            String physical = virtualTopics.toPhysical(virtual);
            if (!Objects.equals(physical, virtual)) {
                state.record(physical, virtual);
                topic.setTopic(physical);
            }
        }
    }

    @Override
    public void onResponse(
            GatewayContext context,
            OffsetForLeaderEpochResponseData data
    ) {
        VirtualTopicState state = VirtualTopicState.from(context);
        for (OffsetForLeaderEpochResponseData.OffsetForLeaderTopicResult topic : data.topics()) {
            String virtual = state.virtualFor(topic.topic());
            if (virtual != null) {
                topic.setTopic(virtual);
            }
        }
    }
}

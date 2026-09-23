package io.jonasg.kawa.virtualtopic.transform;

import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import io.jonasg.kawa.virtualtopic.VirtualTopicState;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData;
import org.apache.kafka.common.message.TxnOffsetCommitResponseData;

import java.util.Objects;

public final class TxnOffsetCommitVirtualTopicTransform
        implements VirtualTopicTransform<TxnOffsetCommitRequestData, TxnOffsetCommitResponseData> {

    private final VirtualTopicManager virtualTopics;

    public TxnOffsetCommitVirtualTopicTransform(VirtualTopicManager virtualTopics) {
        this.virtualTopics = virtualTopics;
    }

    @Override
    public short apiKey() {
        return 28;
    }

    @Override
    public void onRequest(
            GatewayContext context,
            TxnOffsetCommitRequestData data
    ) {
        VirtualTopicState state = VirtualTopicState.from(context);
        for (TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic topic : data.topics()) {
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
            TxnOffsetCommitResponseData data
    ) {
        VirtualTopicState state = VirtualTopicState.from(context);
        for (TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic topic : data.topics()) {
            String virtual = state.virtualFor(topic.name());
            if (virtual != null) {
                topic.setName(virtual);
            }
        }
    }
}

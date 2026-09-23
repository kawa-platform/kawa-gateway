package io.jonasg.kawa.virtualtopic.transform;

import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import io.jonasg.kawa.virtualtopic.VirtualTopicState;
import org.apache.kafka.common.message.OffsetDeleteRequestData;
import org.apache.kafka.common.message.OffsetDeleteResponseData;

import java.util.Objects;

/// Rewrites OffsetDelete topic names between virtual and physical names. The group id and
/// partition indexes are left untouched.
public final class OffsetDeleteVirtualTopicTransform
        implements VirtualTopicTransform<OffsetDeleteRequestData, OffsetDeleteResponseData> {

    private static final short OFFSET_DELETE = 47;

    private final VirtualTopicManager virtualTopics;

    public OffsetDeleteVirtualTopicTransform(VirtualTopicManager virtualTopics) {
        this.virtualTopics = virtualTopics;
    }

    @Override
    public short apiKey() {
        return OFFSET_DELETE;
    }

    @Override
    public void onRequest(
            GatewayContext context,
            OffsetDeleteRequestData data
    ) {
        VirtualTopicState state = VirtualTopicState.from(context);
        for (OffsetDeleteRequestData.OffsetDeleteRequestTopic topic : data.topics()) {
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
            OffsetDeleteResponseData data
    ) {
        VirtualTopicState state = VirtualTopicState.from(context);
        for (OffsetDeleteResponseData.OffsetDeleteResponseTopic topic : data.topics()) {
            String virtual = state.virtualFor(topic.name());
            if (virtual != null) {
                topic.setName(virtual);
            }
        }
    }
}

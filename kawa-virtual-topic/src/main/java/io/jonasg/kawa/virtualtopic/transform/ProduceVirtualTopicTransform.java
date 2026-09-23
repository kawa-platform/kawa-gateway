package io.jonasg.kawa.virtualtopic.transform;

import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import io.jonasg.kawa.virtualtopic.VirtualTopicState;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;

import java.util.Objects;

public final class ProduceVirtualTopicTransform
        implements VirtualTopicTransform<ProduceRequestData, ProduceResponseData> {

    private final VirtualTopicManager virtualTopics;

    public ProduceVirtualTopicTransform(VirtualTopicManager virtualTopics) {
        this.virtualTopics = virtualTopics;
    }

    @Override
    public short apiKey() {
        return 0;
    }

    @Override
    public void onRequest(
            GatewayContext context,
            ProduceRequestData data
    ) {
        VirtualTopicState state = VirtualTopicState.from(context);
        for (ProduceRequestData.TopicProduceData topic : data.topicData()) {
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
            ProduceResponseData data
    ) {
        VirtualTopicState state = VirtualTopicState.from(context);
        for (ProduceResponseData.TopicProduceResponse topic : data.responses()) {
            String virtual = state.virtualFor(topic.name());
            if (virtual != null) {
                topic.setName(virtual);
            }
        }
    }
}

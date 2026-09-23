package io.jonasg.kawa.virtualtopic.transform;

import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import io.jonasg.kawa.virtualtopic.VirtualTopicState;
import org.apache.kafka.common.message.CreatePartitionsRequestData;
import org.apache.kafka.common.message.CreatePartitionsResponseData;

public final class CreatePartitionsVirtualTopicTransform
        implements VirtualTopicTransform<CreatePartitionsRequestData, CreatePartitionsResponseData> {

    private final VirtualTopicManager virtualTopics;

    public CreatePartitionsVirtualTopicTransform(VirtualTopicManager virtualTopics) {
        this.virtualTopics = virtualTopics;
    }

    @Override
    public short apiKey() {
        return 37;
    }

    @Override
    public void onRequest(
            GatewayContext context,
            CreatePartitionsRequestData data
    ) {
        VirtualTopicState state = VirtualTopicState.from(context);
        for (CreatePartitionsRequestData.CreatePartitionsTopic topic : data.topics()) {
            String virtual = topic.name();
            String physical = virtualTopics.toPhysical(virtual);
            if (!physical.equals(virtual)) {
                topic.setName(physical);
                state.record(physical, virtual);
            }
        }
    }

    @Override
    public void onResponse(
            GatewayContext context,
            CreatePartitionsResponseData data
    ) {
        VirtualTopicState state = VirtualTopicState.from(context);
        for (CreatePartitionsResponseData.CreatePartitionsTopicResult topic : data.results()) {
            String virtual = state.virtualFor(topic.name());
            if (virtual != null) {
                topic.setName(virtual);
            }
        }
    }
}

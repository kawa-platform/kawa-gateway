package io.jonasg.kawa.virtualtopic.transform;

import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import io.jonasg.kawa.virtualtopic.VirtualTopicState;
import org.apache.kafka.common.message.DescribeTopicPartitionsRequestData;

import java.util.Objects;

public final class DescribeTopicPartitionsVirtualTopicTransform
        implements VirtualTopicTransform<DescribeTopicPartitionsRequestData, Object> {

    private static final short DESCRIBE_TOPIC_PARTITIONS = 75;

    private final VirtualTopicManager virtualTopics;

    public DescribeTopicPartitionsVirtualTopicTransform(VirtualTopicManager virtualTopics) {
        this.virtualTopics = virtualTopics;
    }

    @Override
    public short apiKey() {
        return DESCRIBE_TOPIC_PARTITIONS;
    }

    @Override
    public void onRequest(
            GatewayContext context,
            DescribeTopicPartitionsRequestData data
    ) {
        var state = VirtualTopicState.from(context);
        for (DescribeTopicPartitionsRequestData.TopicRequest topic : data.topics()) {
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
            Object body
    ) {
    }
}

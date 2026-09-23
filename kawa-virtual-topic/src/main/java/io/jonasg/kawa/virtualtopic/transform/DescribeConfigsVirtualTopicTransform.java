package io.jonasg.kawa.virtualtopic.transform;

import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import io.jonasg.kawa.virtualtopic.VirtualTopicState;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.message.DescribeConfigsRequestData;
import org.apache.kafka.common.message.DescribeConfigsResponseData;

import java.util.Objects;

/// Rewrites DescribeConfigs topic-resource names between virtual and physical topic names.
/// Non-topic resources (brokers, broker loggers) pass through unchanged.
public final class DescribeConfigsVirtualTopicTransform
        implements VirtualTopicTransform<DescribeConfigsRequestData, DescribeConfigsResponseData> {

    private static final short DESCRIBE_CONFIGS = 32;

    private final VirtualTopicManager virtualTopics;

    public DescribeConfigsVirtualTopicTransform(VirtualTopicManager virtualTopics) {
        this.virtualTopics = virtualTopics;
    }

    @Override
    public short apiKey() {
        return DESCRIBE_CONFIGS;
    }

    @Override
    public void onRequest(
            GatewayContext context,
            DescribeConfigsRequestData data
    ) {
        VirtualTopicState state = VirtualTopicState.from(context);
        for (DescribeConfigsRequestData.DescribeConfigsResource resource : data.resources()) {
            if (resource.resourceType() != ConfigResource.Type.TOPIC.id()) {
                continue;
            }
            String physical = virtualTopics.toPhysical(resource.resourceName());
            if (!Objects.equals(physical, resource.resourceName())) {
                state.record(physical, resource.resourceName());
                resource.setResourceName(physical);
            }
        }
    }

    @Override
    public void onResponse(
            GatewayContext context,
            DescribeConfigsResponseData data
    ) {
        VirtualTopicState state = VirtualTopicState.from(context);
        for (DescribeConfigsResponseData.DescribeConfigsResult result : data.results()) {
            if (result.resourceType() != ConfigResource.Type.TOPIC.id()) {
                continue;
            }
            String virtual = state.virtualFor(result.resourceName());
            if (virtual != null) {
                result.setResourceName(virtual);
            }
        }
    }
}

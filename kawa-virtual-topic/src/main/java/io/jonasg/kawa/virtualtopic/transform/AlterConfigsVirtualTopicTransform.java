package io.jonasg.kawa.virtualtopic.transform;

import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import io.jonasg.kawa.virtualtopic.VirtualTopicState;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.message.AlterConfigsRequestData;
import org.apache.kafka.common.message.AlterConfigsResponseData;

import java.util.Objects;

/// Rewrites AlterConfigs topic-resource names between virtual and physical names. Non-topic
/// resources (brokers, broker loggers) pass through unchanged. Config entries and the
/// validate-only flag are left untouched.
public final class AlterConfigsVirtualTopicTransform
        implements VirtualTopicTransform<AlterConfigsRequestData, AlterConfigsResponseData> {

    private static final short ALTER_CONFIGS = 33;

    private final VirtualTopicManager virtualTopics;

    public AlterConfigsVirtualTopicTransform(VirtualTopicManager virtualTopics) {
        this.virtualTopics = virtualTopics;
    }

    @Override
    public short apiKey() {
        return ALTER_CONFIGS;
    }

    @Override
    public void onRequest(
            GatewayContext context,
            AlterConfigsRequestData data
    ) {
        VirtualTopicState state = VirtualTopicState.from(context);
        for (AlterConfigsRequestData.AlterConfigsResource resource : data.resources()) {
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
            AlterConfigsResponseData data
    ) {
        VirtualTopicState state = VirtualTopicState.from(context);
        for (AlterConfigsResponseData.AlterConfigsResourceResponse response : data.responses()) {
            if (response.resourceType() != ConfigResource.Type.TOPIC.id()) {
                continue;
            }
            String virtual = state.virtualFor(response.resourceName());
            if (virtual != null) {
                response.setResourceName(virtual);
            }
        }
    }
}

package io.jonasg.kawa.virtualtopic.transform;

import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import io.jonasg.kawa.virtualtopic.VirtualTopicState;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData;
import org.apache.kafka.common.message.IncrementalAlterConfigsResponseData;

import java.util.Objects;

/// Rewrites IncrementalAlterConfigs topic-resource names between virtual and physical names.
/// Non-topic resources (brokers, broker loggers) pass through unchanged. Config entries and
/// their operation types (SET/DELETE/APPEND/SUBTRACT) are left untouched.
public final class IncrementalAlterConfigsVirtualTopicTransform
        implements VirtualTopicTransform<IncrementalAlterConfigsRequestData, IncrementalAlterConfigsResponseData> {

    private static final short INCREMENTAL_ALTER_CONFIGS = 44;

    private final VirtualTopicManager virtualTopics;

    public IncrementalAlterConfigsVirtualTopicTransform(VirtualTopicManager virtualTopics) {
        this.virtualTopics = virtualTopics;
    }

    @Override
    public short apiKey() {
        return INCREMENTAL_ALTER_CONFIGS;
    }

    @Override
    public void onRequest(
            GatewayContext context,
            IncrementalAlterConfigsRequestData data
    ) {
        VirtualTopicState state = VirtualTopicState.from(context);
        for (IncrementalAlterConfigsRequestData.AlterConfigsResource resource : data.resources()) {
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
            IncrementalAlterConfigsResponseData data
    ) {
        VirtualTopicState state = VirtualTopicState.from(context);
        for (IncrementalAlterConfigsResponseData.AlterConfigsResourceResponse response : data.responses()) {
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

package io.jonasg.kawa.virtualtopic.transform;

import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import io.jonasg.kawa.virtualtopic.VirtualTopicState;
import org.apache.kafka.common.message.DescribeAclsRequestData;
import org.apache.kafka.common.message.DescribeAclsResponseData;
import org.apache.kafka.common.resource.ResourceType;

import java.util.Objects;

/// Rewrites DescribeAcls topic-resource filters and descriptions between virtual and physical
/// topic names. Non-topic resources pass through unchanged; a null resource-name filter (any
/// name) is left untouched.
public final class DescribeAclsVirtualTopicTransform
        implements VirtualTopicTransform<DescribeAclsRequestData, DescribeAclsResponseData> {

    private static final short DESCRIBE_ACLS = 29;

    private final VirtualTopicManager virtualTopics;

    public DescribeAclsVirtualTopicTransform(VirtualTopicManager virtualTopics) {
        this.virtualTopics = virtualTopics;
    }

    @Override
    public short apiKey() {
        return DESCRIBE_ACLS;
    }

    @Override
    public void onRequest(
            GatewayContext context,
            DescribeAclsRequestData data
    ) {
        if (data.resourceTypeFilter() != ResourceType.TOPIC.code() || data.resourceNameFilter() == null) {
            return;
        }
        String physical = virtualTopics.toPhysical(data.resourceNameFilter());
        if (!Objects.equals(physical, data.resourceNameFilter())) {
            VirtualTopicState.from(context).record(physical, data.resourceNameFilter());
            data.setResourceNameFilter(physical);
        }
    }

    @Override
    public void onResponse(
            GatewayContext context,
            DescribeAclsResponseData data
    ) {
        VirtualTopicState state = VirtualTopicState.from(context);
        for (DescribeAclsResponseData.DescribeAclsResource resource : data.resources()) {
            if (resource.resourceType() != ResourceType.TOPIC.code()) {
                continue;
            }
            String virtual = state.virtualFor(resource.resourceName());
            if (virtual != null) {
                resource.setResourceName(virtual);
            }
        }
    }
}

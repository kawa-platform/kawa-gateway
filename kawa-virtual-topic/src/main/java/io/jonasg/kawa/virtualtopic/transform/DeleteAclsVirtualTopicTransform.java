package io.jonasg.kawa.virtualtopic.transform;

import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import io.jonasg.kawa.virtualtopic.VirtualTopicState;
import org.apache.kafka.common.message.DeleteAclsRequestData;
import org.apache.kafka.common.message.DeleteAclsResponseData;
import org.apache.kafka.common.resource.ResourceType;

import java.util.Objects;

/// Rewrites DeleteAcls topic-resource filters and matching-acl descriptions between virtual and
/// physical topic names. Non-topic resources pass through unchanged; a null resource-name filter
/// (any name) is left untouched.
public final class DeleteAclsVirtualTopicTransform
        implements VirtualTopicTransform<DeleteAclsRequestData, DeleteAclsResponseData> {

    private static final short DELETE_ACLS = 31;

    private final VirtualTopicManager virtualTopics;

    public DeleteAclsVirtualTopicTransform(VirtualTopicManager virtualTopics) {
        this.virtualTopics = virtualTopics;
    }

    @Override
    public short apiKey() {
        return DELETE_ACLS;
    }

    @Override
    public void onRequest(
            GatewayContext context,
            DeleteAclsRequestData data
    ) {
        VirtualTopicState state = VirtualTopicState.from(context);
        for (DeleteAclsRequestData.DeleteAclsFilter filter : data.filters()) {
            if (filter.resourceTypeFilter() != ResourceType.TOPIC.code() || filter.resourceNameFilter() == null) {
                continue;
            }
            String physical = virtualTopics.toPhysical(filter.resourceNameFilter());
            if (!Objects.equals(physical, filter.resourceNameFilter())) {
                state.record(physical, filter.resourceNameFilter());
                filter.setResourceNameFilter(physical);
            }
        }
    }

    @Override
    public void onResponse(
            GatewayContext context,
            DeleteAclsResponseData data
    ) {
        VirtualTopicState state = VirtualTopicState.from(context);
        for (DeleteAclsResponseData.DeleteAclsFilterResult result : data.filterResults()) {
            for (DeleteAclsResponseData.DeleteAclsMatchingAcl acl : result.matchingAcls()) {
                if (acl.resourceType() != ResourceType.TOPIC.code()) {
                    continue;
                }
                String virtual = state.virtualFor(acl.resourceName());
                if (virtual != null) {
                    acl.setResourceName(virtual);
                }
            }
        }
    }
}

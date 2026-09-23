package io.jonasg.kawa.virtualtopic.transform;

import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import io.jonasg.kawa.virtualtopic.VirtualTopicState;
import org.apache.kafka.common.message.CreateAclsRequestData;
import org.apache.kafka.common.message.CreateAclsResponseData;
import org.apache.kafka.common.resource.ResourceType;

import java.util.Objects;

/// Rewrites CreateAcls topic-resource names between virtual and physical topic names. Non-topic
/// resources pass through unchanged; the response carries only per-entry statuses.
public final class CreateAclsVirtualTopicTransform
        implements VirtualTopicTransform<CreateAclsRequestData, CreateAclsResponseData> {

    private static final short CREATE_ACLS = 30;

    private final VirtualTopicManager virtualTopics;

    public CreateAclsVirtualTopicTransform(VirtualTopicManager virtualTopics) {
        this.virtualTopics = virtualTopics;
    }

    @Override
    public short apiKey() {
        return CREATE_ACLS;
    }

    @Override
    public void onRequest(
            GatewayContext context,
            CreateAclsRequestData data
    ) {
        VirtualTopicState state = VirtualTopicState.from(context);
        for (CreateAclsRequestData.AclCreation creation : data.creations()) {
            if (creation.resourceType() != ResourceType.TOPIC.code()) {
                continue;
            }
            String physical = virtualTopics.toPhysical(creation.resourceName());
            if (!Objects.equals(physical, creation.resourceName())) {
                state.record(physical, creation.resourceName());
                creation.setResourceName(physical);
            }
        }
    }

    @Override
    public void onResponse(
            GatewayContext context,
            CreateAclsResponseData data
    ) {
    }
}

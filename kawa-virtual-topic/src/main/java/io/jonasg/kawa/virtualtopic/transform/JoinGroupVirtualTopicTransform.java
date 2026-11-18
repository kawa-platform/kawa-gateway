package io.jonasg.kawa.virtualtopic.transform;

import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.core.ShortCircuitResult;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import io.jonasg.kawa.virtualtopic.VirtualTopicState;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;

/// Refuses a classic consumer joining a group with a subscription that names two virtual topics on one physical
/// topic: the group's committed offsets are kept per physical partition, so the two would overwrite each other.
/// The subscription is not rewritten, since classic consumers subscribe by the names Metadata shows them.
/// Non-`consumer` protocol types and metadata that does not decode as a consumer subscription pass through.
public final class JoinGroupVirtualTopicTransform
        implements VirtualTopicTransform<JoinGroupRequestData, JoinGroupResponseData> {

    private static final String CONSUMER_PROTOCOL_TYPE = "consumer";

    private final VirtualTopicManager virtualTopics;

    public JoinGroupVirtualTopicTransform(VirtualTopicManager virtualTopics) {
        this.virtualTopics = virtualTopics;
    }

    @Override
    public short apiKey() {
        return 11;
    }

    @Override
    public void onRequest(
            GatewayContext context,
            JoinGroupRequestData data
    ) {
        if (!CONSUMER_PROTOCOL_TYPE.equals(data.protocolType())) {
            return;
        }
        for (JoinGroupRequestData.JoinGroupRequestProtocol protocol : data.protocols()) {
            ConsumerPartitionAssignor.Subscription subscription;
            try {
                subscription = ConsumerProtocol.deserializeSubscription(ByteBuffer.wrap(protocol.metadata()));
            } catch (RuntimeException e) {
                continue;
            }
            if (!virtualTopics.sharedPhysicalTopics(subscription.topics()).isEmpty()) {
                var refusal = new JoinGroupResponseData().setErrorCode(Errors.INVALID_REQUEST.code());
                context.shortCircuit(new ShortCircuitResult(apiKey(), VirtualTopicState.from(context).apiVersion(), refusal));
                return;
            }
        }
    }

    @Override
    public void onResponse(
            GatewayContext context,
            JoinGroupResponseData data
    ) {
    }
}

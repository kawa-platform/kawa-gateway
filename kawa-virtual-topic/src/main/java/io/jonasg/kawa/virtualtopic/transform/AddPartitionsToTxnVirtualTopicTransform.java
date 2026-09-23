package io.jonasg.kawa.virtualtopic.transform;

import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import io.jonasg.kawa.virtualtopic.VirtualTopicState;
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData;
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData;

import java.util.Objects;

/// Rewrites the partition-list topic names of AddPartitionsToTxn between virtual and physical
/// names. Registered for versions 0-3 only: those are the versions clients use; v4+ carries a
/// different, broker-internal schema. Transactional metadata (transactional id, producer id and
/// epoch) is left untouched.
public final class AddPartitionsToTxnVirtualTopicTransform
        implements VirtualTopicTransform<AddPartitionsToTxnRequestData, AddPartitionsToTxnResponseData> {

    private static final short ADD_PARTITIONS_TO_TXN = 24;

    private final VirtualTopicManager virtualTopics;

    public AddPartitionsToTxnVirtualTopicTransform(VirtualTopicManager virtualTopics) {
        this.virtualTopics = virtualTopics;
    }

    @Override
    public short apiKey() {
        return ADD_PARTITIONS_TO_TXN;
    }

    @Override
    public void onRequest(
            GatewayContext context,
            AddPartitionsToTxnRequestData data
    ) {
        VirtualTopicState state = VirtualTopicState.from(context);
        for (AddPartitionsToTxnRequestData.AddPartitionsToTxnTopic topic : data.v3AndBelowTopics()) {
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
            AddPartitionsToTxnResponseData data
    ) {
        VirtualTopicState state = VirtualTopicState.from(context);
        for (AddPartitionsToTxnResponseData.AddPartitionsToTxnTopicResult result
                : data.resultsByTopicV3AndBelow()) {
            String virtual = state.virtualFor(result.name());
            if (virtual != null) {
                result.setName(virtual);
            }
        }
    }
}

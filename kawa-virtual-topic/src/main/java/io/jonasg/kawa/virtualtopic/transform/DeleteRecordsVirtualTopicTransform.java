package io.jonasg.kawa.virtualtopic.transform;

import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import io.jonasg.kawa.virtualtopic.VirtualTopicState;
import org.apache.kafka.common.message.DeleteRecordsRequestData;
import org.apache.kafka.common.message.DeleteRecordsResponseData;

import java.util.Objects;

/// Rewrites DeleteRecords topic names between virtual and physical names. The timeout,
/// partition indexes, record offsets and per-partition low watermarks are left untouched.
public final class DeleteRecordsVirtualTopicTransform
        implements VirtualTopicTransform<DeleteRecordsRequestData, DeleteRecordsResponseData> {

    private static final short DELETE_RECORDS = 21;

    private final VirtualTopicManager virtualTopics;

    public DeleteRecordsVirtualTopicTransform(VirtualTopicManager virtualTopics) {
        this.virtualTopics = virtualTopics;
    }

    @Override
    public short apiKey() {
        return DELETE_RECORDS;
    }

    @Override
    public void onRequest(
            GatewayContext context,
            DeleteRecordsRequestData data
    ) {
        VirtualTopicState state = VirtualTopicState.from(context);
        for (DeleteRecordsRequestData.DeleteRecordsTopic topic : data.topics()) {
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
            DeleteRecordsResponseData data
    ) {
        VirtualTopicState state = VirtualTopicState.from(context);
        for (DeleteRecordsResponseData.DeleteRecordsTopicResult result : data.topics()) {
            String virtual = state.virtualFor(result.name());
            if (virtual != null) {
                result.setName(virtual);
            }
        }
    }
}

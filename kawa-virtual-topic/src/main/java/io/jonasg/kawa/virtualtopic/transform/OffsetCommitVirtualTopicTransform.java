package io.jonasg.kawa.virtualtopic.transform;

import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.core.ShortCircuitResult;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import io.jonasg.kawa.virtualtopic.VirtualTopicState;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.protocol.Errors;

import java.util.Objects;

public final class OffsetCommitVirtualTopicTransform
        implements VirtualTopicTransform<OffsetCommitRequestData, OffsetCommitResponseData> {

    private final VirtualTopicManager virtualTopics;

    public OffsetCommitVirtualTopicTransform(VirtualTopicManager virtualTopics) {
        this.virtualTopics = virtualTopics;
    }

    @Override
    public short apiKey() {
        return 8;
    }

    @Override
    public void onRequest(
            GatewayContext context,
            OffsetCommitRequestData data
    ) {
        VirtualTopicState state = VirtualTopicState.from(context);
        if (!virtualTopics.sharedPhysicalTopics(data.topics().stream().map(OffsetCommitRequestData.OffsetCommitRequestTopic::name).toList()).isEmpty()) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), state.apiVersion(), refusal(data)));
            return;
        }
        for (OffsetCommitRequestData.OffsetCommitRequestTopic topic : data.topics()) {
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
            OffsetCommitResponseData data
    ) {
        VirtualTopicState state = VirtualTopicState.from(context);
        for (OffsetCommitResponseData.OffsetCommitResponseTopic topic : data.topics()) {
            String virtual = state.virtualFor(topic.name());
            if (virtual != null) {
                topic.setName(virtual);
            }
        }
    }

    /// Answers every topic and partition the request named, under the client's names, with [Errors#INVALID_REQUEST].
    private static OffsetCommitResponseData refusal(OffsetCommitRequestData data) {
        var response = new OffsetCommitResponseData();
        for (OffsetCommitRequestData.OffsetCommitRequestTopic topic : data.topics()) {
            var topicResponse = new OffsetCommitResponseData.OffsetCommitResponseTopic().setName(topic.name());
            for (OffsetCommitRequestData.OffsetCommitRequestPartition partition : topic.partitions()) {
                topicResponse.partitions().add(new OffsetCommitResponseData.OffsetCommitResponsePartition()
                        .setPartitionIndex(partition.partitionIndex())
                        .setErrorCode(Errors.INVALID_REQUEST.code()));
            }
            response.topics().add(topicResponse);
        }
        return response;
    }
}

package io.jonasg.kawa.virtualtopic.transform;

import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.core.ShortCircuitResult;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import io.jonasg.kawa.virtualtopic.VirtualTopicState;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.protocol.Errors;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class OffsetFetchVirtualTopicTransform
        implements VirtualTopicTransform<OffsetFetchRequestData, OffsetFetchResponseData> {

    private final VirtualTopicManager virtualTopics;

    public OffsetFetchVirtualTopicTransform(VirtualTopicManager virtualTopics) {
        this.virtualTopics = virtualTopics;
    }

    @Override
    public short apiKey() {
        return 9;
    }

    @Override
    public void onRequest(
            GatewayContext context,
            OffsetFetchRequestData data
    ) {
        VirtualTopicState state = VirtualTopicState.from(context);
        state.offsetFetchAllTopics(data.topics() == null);
        if (data.topics() == null) {
            return;
        }
        if (!virtualTopics.sharedPhysicalTopics(data.topics().stream().map(OffsetFetchRequestData.OffsetFetchRequestTopic::name).toList()).isEmpty()) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), state.apiVersion(), refusal(data)));
            return;
        }
        for (OffsetFetchRequestData.OffsetFetchRequestTopic topic : data.topics()) {
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
            OffsetFetchResponseData data
    ) {
        VirtualTopicState state = VirtualTopicState.from(context);
        // A physical topic with several virtual topics is answered once per virtual topic: the first takes over
        // this entry and every other one is appended to data.topics(). Iterate a copy, since appending to the
        // list being iterated throws ConcurrentModificationException.
        for (OffsetFetchResponseData.OffsetFetchResponseTopic topic : new ArrayList<>(data.topics())) {
            String virtual = state.virtualFor(topic.name());
            if (virtual != null) {
                topic.setName(virtual);
            } else if (state.offsetFetchAllTopics()) {
                List<String> names = virtualTopics.toVirtual(topic.name());
                for (String name : names.subList(1, names.size())) {
                    data.topics().add(topic.duplicate().setName(name));
                }
                topic.setName(names.getFirst());
            }
        }
    }

    /// Answers every topic and partition the request named, under the client's names, with [Errors#INVALID_REQUEST].
    private static OffsetFetchResponseData refusal(OffsetFetchRequestData data) {
        var response = new OffsetFetchResponseData();
        for (OffsetFetchRequestData.OffsetFetchRequestTopic topic : data.topics()) {
            var topicResponse = new OffsetFetchResponseData.OffsetFetchResponseTopic().setName(topic.name());
            for (Integer partitionIndex : topic.partitionIndexes()) {
                topicResponse.partitions().add(new OffsetFetchResponseData.OffsetFetchResponsePartition()
                        .setPartitionIndex(partitionIndex)
                        .setCommittedOffset(-1L)
                        .setErrorCode(Errors.INVALID_REQUEST.code()));
            }
            response.topics().add(topicResponse);
        }
        return response;
    }
}

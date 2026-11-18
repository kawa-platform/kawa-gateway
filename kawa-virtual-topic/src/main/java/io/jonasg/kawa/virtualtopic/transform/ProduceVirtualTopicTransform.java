package io.jonasg.kawa.virtualtopic.transform;

import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.core.ShortCircuitResult;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import io.jonasg.kawa.virtualtopic.VirtualTopicState;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.Errors;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public final class ProduceVirtualTopicTransform
        implements VirtualTopicTransform<ProduceRequestData, ProduceResponseData> {

    private final VirtualTopicManager virtualTopics;

    public ProduceVirtualTopicTransform(VirtualTopicManager virtualTopics) {
        this.virtualTopics = virtualTopics;
    }

    @Override
    public short apiKey() {
        return 0;
    }

    @Override
    public void onRequest(
            GatewayContext context,
            ProduceRequestData data
    ) {
        VirtualTopicState state = VirtualTopicState.from(context);
        Map<String, Set<String>> shared = virtualTopics.sharedPhysicalTopics(
                data.topicData().stream().map(ProduceRequestData.TopicProduceData::name).toList());
        if (!shared.isEmpty()) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), state.apiVersion(), refusal(data, refusalMessage(shared))));
            return;
        }
        for (ProduceRequestData.TopicProduceData topic : data.topicData()) {
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
            ProduceResponseData data
    ) {
        VirtualTopicState state = VirtualTopicState.from(context);
        for (ProduceResponseData.TopicProduceResponse topic : data.responses()) {
            String virtual = state.virtualFor(topic.name());
            if (virtual != null) {
                topic.setName(virtual);
            }
        }
    }

    /// Answers every topic and partition the request named, under the client's names, with [Errors#INVALID_REQUEST].
    private static ProduceResponseData refusal(ProduceRequestData data, String message) {
        var response = new ProduceResponseData();
        for (ProduceRequestData.TopicProduceData topic : data.topicData()) {
            var topicResponse = new ProduceResponseData.TopicProduceResponse().setName(topic.name());
            for (ProduceRequestData.PartitionProduceData partition : topic.partitionData()) {
                topicResponse.partitionResponses().add(new ProduceResponseData.PartitionProduceResponse()
                        .setIndex(partition.index())
                        .setErrorCode(Errors.INVALID_REQUEST.code())
                        .setErrorMessage(message));
            }
            response.responses().add(topicResponse);
        }
        return response;
    }

    private static String refusalMessage(Map<String, Set<String>> shared) {
        return shared.entrySet().stream()
                .map(entry -> "virtual topics " + entry.getValue() + " share physical topic '" + entry.getKey() + "'")
                .collect(Collectors.joining("; ", "", "; a request may name only one virtual topic per physical topic"));
    }
}

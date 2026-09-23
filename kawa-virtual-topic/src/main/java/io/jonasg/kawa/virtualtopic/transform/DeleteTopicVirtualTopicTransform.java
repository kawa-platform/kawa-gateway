package io.jonasg.kawa.virtualtopic.transform;

import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.core.ShortCircuitResult;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import io.jonasg.kawa.virtualtopic.VirtualTopicState;
import org.apache.kafka.common.message.DeleteTopicsRequestData;
import org.apache.kafka.common.message.DeleteTopicsResponseData;
import org.apache.kafka.common.protocol.Errors;

import java.util.ArrayList;
import java.util.List;

/// Guards the virtual-topic namespace for DeleteTopics: virtual names are reserved and must
/// be rejected locally, while physical topic names are forwarded to the broker unchanged.
public final class DeleteTopicVirtualTopicTransform
        implements VirtualTopicTransform<DeleteTopicsRequestData, DeleteTopicsResponseData> {

    private final VirtualTopicManager virtualTopics;

    public DeleteTopicVirtualTopicTransform(VirtualTopicManager virtualTopics) {
        this.virtualTopics = virtualTopics;
    }

    @Override
    public short apiKey() {
        return 20;
    }

    @Override
    public void onRequest(
            GatewayContext context,
            DeleteTopicsRequestData data
    ) {
        List<String> rejected = new ArrayList<>();
        for (String name : new ArrayList<>(data.topicNames())) {
            if (virtualTopics.virtualTopics().containsKey(name)) {
                rejected.add(name);
                data.topicNames().remove(name);
            }
        }
        for (DeleteTopicsRequestData.DeleteTopicState topic : new ArrayList<>(data.topics())) {
            String name = topic.name();
            if (name != null && virtualTopics.virtualTopics().containsKey(name)) {
                rejected.add(name);
                data.topics().remove(topic);
            }
        }
        if (rejected.isEmpty()) {
            return;
        }
        VirtualTopicState state = VirtualTopicState.from(context);
        state.rejectedDeleteTopics(rejected);
        if (data.topicNames().isEmpty() && data.topics().isEmpty()) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), state.apiVersion(), errorResponse(rejected)));
        }
    }

    @Override
    public void onResponse(
            GatewayContext context,
            DeleteTopicsResponseData data
    ) {
        VirtualTopicState state = VirtualTopicState.from(context);
        for (String name : state.rejectedDeleteTopics()) {
            data.responses().add(rejectionResult(name));
        }
    }

    private DeleteTopicsResponseData errorResponse(List<String> rejected) {
        var responseData = new DeleteTopicsResponseData();
        for (String name : rejected) {
            responseData.responses().add(rejectionResult(name));
        }
        return responseData;
    }

    private DeleteTopicsResponseData.DeletableTopicResult rejectionResult(String name) {
        return new DeleteTopicsResponseData.DeletableTopicResult()
                .setName(name)
                .setErrorCode(Errors.INVALID_REQUEST.code())
                .setErrorMessage("virtual topic '" + name + "' is reserved; use '"
                                 + virtualTopics.toPhysical(name) + "'");
    }
}

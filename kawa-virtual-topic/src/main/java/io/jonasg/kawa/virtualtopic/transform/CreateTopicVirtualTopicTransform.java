package io.jonasg.kawa.virtualtopic.transform;

import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.core.ShortCircuitResult;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import io.jonasg.kawa.virtualtopic.VirtualTopicState;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.protocol.Errors;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/// Guards the virtual-topic namespace: a CreateTopics request that names a virtual
/// topic is answered locally with a per-topic [Errors.INVALID_REQUEST] error instead of
/// being forwarded to a broker, so clients cannot create topics that shadow the virtual map.
/// Non-alias topics in the same request are forwarded unchanged (aliases dropped), mirroring
/// Kafka's per-topic independent CreateTopics semantics.
public final class CreateTopicVirtualTopicTransform
        implements VirtualTopicTransform<CreateTopicsRequestData, CreateTopicsResponseData> {

    private final VirtualTopicManager virtualTopics;

    public CreateTopicVirtualTopicTransform(VirtualTopicManager virtualTopics) {
        this.virtualTopics = virtualTopics;
    }

    @Override
    public short apiKey() {
        return 19;
    }

    @Override
    public void onRequest(
            GatewayContext context,
            CreateTopicsRequestData data
    ) {
        List<String> rejected = new ArrayList<>();
        for (CreateTopicsRequestData.CreatableTopic topic : new ArrayList<>(data.topics())) {
            String physical = virtualTopics.toPhysical(topic.name());
            if (!Objects.equals(physical, topic.name())) {
                rejected.add(topic.name());
                data.topics().remove(topic);
            }
        }
        if (rejected.isEmpty()) {
            return;
        }
        VirtualTopicState state = VirtualTopicState.from(context);
        state.rejectedCreateTopics(rejected);
        if (data.topics().isEmpty()) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), state.apiVersion(), errorResponse(rejected)));
        }
    }

    @Override
    public void onResponse(
            GatewayContext context,
            CreateTopicsResponseData data
    ) {
        VirtualTopicState state = VirtualTopicState.from(context);
        for (String name : state.rejectedCreateTopics()) {
            data.topics().add(rejectionResult(name));
        }
    }

    private CreateTopicsResponseData errorResponse(List<String> rejected) {
        var responseData = new CreateTopicsResponseData();
        for (String name : rejected) {
            responseData.topics().add(rejectionResult(name));
        }
        return responseData;
    }

    private CreateTopicsResponseData.CreatableTopicResult rejectionResult(String name) {
        return new CreateTopicsResponseData.CreatableTopicResult()
                .setName(name)
                .setErrorCode(Errors.INVALID_REQUEST.code())
                .setErrorMessage("virtual topic '" + name + "' is reserved; use '"
                                 + virtualTopics.toPhysical(name) + "'");
    }
}

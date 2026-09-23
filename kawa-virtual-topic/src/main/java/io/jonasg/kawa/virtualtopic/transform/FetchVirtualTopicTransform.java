package io.jonasg.kawa.virtualtopic.transform;

import io.jonasg.kawa.config.VirtualTopicFilterConfig;
import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import io.jonasg.kawa.virtualtopic.filter.VirtualTopicRecordFilter;
import io.jonasg.kawa.virtualtopic.FetchSessionRegistry;
import io.jonasg.kawa.virtualtopic.VirtualTopicState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.protocol.Errors;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public final class FetchVirtualTopicTransform
        implements VirtualTopicTransform<FetchRequestData, FetchResponseData> {

    private static final int FINAL_EPOCH = -1;

    private final VirtualTopicManager virtualTopics;
    private final FetchSessionRegistry fetchSessions;
    private final VirtualTopicRecordFilter recordFilter;

    public FetchVirtualTopicTransform(
            VirtualTopicManager virtualTopics,
            FetchSessionRegistry fetchSessions,
            VirtualTopicRecordFilter recordFilter
    ) {
        this.virtualTopics = virtualTopics;
        this.fetchSessions = fetchSessions;
        this.recordFilter = recordFilter;
    }

    @Override
    public short apiKey() {
        return 1;
    }

    @Override
    public void onRequest(
            GatewayContext context,
            FetchRequestData data
    ) {
        VirtualTopicState state = VirtualTopicState.from(context);
        for (FetchRequestData.FetchTopic topic : data.topics()) {
            String physical = virtualTopics.toPhysical(topic.topic());
            if (!Objects.equals(physical, topic.topic())) {
                state.record(physical, topic.topic());
                topic.setTopic(physical);
            }
        }
        for (FetchRequestData.ForgottenTopic forgotten : data.forgottenTopicsData()) {
            forgotten.setTopic(virtualTopics.toPhysical(forgotten.topic()));
        }

        state.fetchSessionId(data.sessionId());
        if (data.sessionId() == 0) {
            return;
        }
        if (data.sessionEpoch() == FINAL_EPOCH) {
            fetchSessions.removeSession(context.source(), data.sessionId());
            return;
        }
        List<String> forgottenPhysical = new ArrayList<>(data.forgottenTopicsData().size());
        for (FetchRequestData.ForgottenTopic forgotten : data.forgottenTopicsData()) {
            forgottenPhysical.add(forgotten.topic());
        }
        fetchSessions.onFetchRequest(
                context.source(), data.sessionId(), state.physicalToVirtual(), forgottenPhysical
        );
    }

    @Override
    public void onResponse(
            GatewayContext context,
            FetchResponseData data
    ) {
        VirtualTopicState state = VirtualTopicState.from(context);
        int sessionId = data.sessionId();
        if (sessionId != 0 && data.errorCode() == Errors.FETCH_SESSION_ID_NOT_FOUND.code()) {
            fetchSessions.removeSession(context.source(), sessionId);
        }
        if (sessionId != 0 && state.fetchSessionId() == 0) {
            fetchSessions.bindSession(context.source(), sessionId, state.physicalToVirtual());
        }
        if (sessionId != 0 && fetchSessions.hasSession(context.source(), sessionId)) {
            for (FetchResponseData.FetchableTopicResponse topic : data.responses()) {
                String virtual = fetchSessions.virtualFor(
                        context.source(), sessionId, topic.topic()
                );
                if (virtual != null) {
                    topic.setTopic(virtual);
                }
            }
            applyConsumeFilters(data);
            return;
        }
        for (FetchResponseData.FetchableTopicResponse topic : data.responses()) {
            String virtual = state.virtualFor(topic.topic());
            if (virtual != null) {
                topic.setTopic(virtual);
            }
        }

        applyConsumeFilters(data);
    }

    private void applyConsumeFilters(FetchResponseData data) {
        for (FetchResponseData.FetchableTopicResponse topic : data.responses()) {
            Optional<VirtualTopicFilterConfig> filter = virtualTopics.filterFor(topic.topic());
            if (filter.isEmpty()) {
                continue;
            }
            var valueFormat = virtualTopics.valueFormatFor(topic.topic()).orElse(null);
            for (FetchResponseData.PartitionData partition : topic.partitions()) {
                TopicPartition tp = new TopicPartition(topic.topic(), partition.partitionIndex());
                partition.setRecords(recordFilter.apply(filter.get(), valueFormat, partition.records()));
            }
        }
    }
}

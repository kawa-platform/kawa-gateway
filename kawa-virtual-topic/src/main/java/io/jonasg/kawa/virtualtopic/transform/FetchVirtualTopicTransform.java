package io.jonasg.kawa.virtualtopic.transform;

import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.core.ShortCircuitResult;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import io.jonasg.kawa.virtualtopic.filter.FailedRecordException;
import io.jonasg.kawa.virtualtopic.filter.RecordBatches;
import io.jonasg.kawa.virtualtopic.filter.SchemaRegistryUnavailableException;
import io.jonasg.kawa.virtualtopic.FetchSessionRegistry;
import io.jonasg.kawa.virtualtopic.VirtualTopicState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.BaseRecords;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;

/// Renames fetched topics between their virtual and physical names, and rewrites the records a
/// virtual topic delivers: its consume filter first, on the stored values, then its
/// transformations on the records that remain. A topic with transformations is filtered and
/// transformed in one pass by its [VirtualTopicRecordTransformer], which decodes each value once
/// for both; a topic with only a filter is filtered by its [VirtualTopicRecordFilter]. Transaction
/// markers are delivered unchanged either way.
///
/// Only the records from the offset the client fetched a partition at are rewritten and delivered:
/// the broker answers with whole batches, from the one holding that offset, and the client discards
/// the records below it. The offset comes from the request, or - for a partition an incremental
/// fetch leaves out because its offset did not change - from the [FetchSessionRegistry].
///
/// Records are rewritten per response from the configuration current at that moment, and each
/// record on its own, so a fetch session needs no knowledge of formats: one response may hold
/// records stored in different formats, and a configuration change applies from the next response
/// of a session on.
///
/// A failure costs only the partition it hits; the other partitions of the response are delivered
/// as usual:
///
/// - A bad record under the `fail` policy ([FailedRecordException]) stops the partition at that
///   record: the records before it are delivered, ending right before it, so the consumer's next
///   fetch starts at it. When it is the first record from the consumer's fetch offset on, the
///   partition answers `CORRUPT_MESSAGE` with no records: the Java consumer then throws a
///   `KafkaException` naming its offset from `poll` until the application seeks past it.
/// - A schema registry outage ([SchemaRegistryUnavailableException]) is not a record error: the
///   partition comes back with `UNKNOWN_SERVER_ERROR` and no records, so the consumer does not move
///   past them and fetches them again once the registry is back.
/// - Any other exception, such as a filter that fails to evaluate or a bug, is no record error
///   either and is not handled per the `onDecodeError` policy: the partition comes back with
///   `UNKNOWN_SERVER_ERROR` and no records, and the failure is logged.
public final class FetchVirtualTopicTransform
        implements VirtualTopicTransform<FetchRequestData, FetchResponseData> {

    private static final Logger log = LoggerFactory.getLogger(FetchVirtualTopicTransform.class);
    private static final int FINAL_EPOCH = -1;

    private final VirtualTopicManager virtualTopicManager;
    private final FetchSessionRegistry fetchSessions;

    public FetchVirtualTopicTransform(VirtualTopicManager virtualTopicManager, FetchSessionRegistry fetchSessions) {
        this.virtualTopicManager = virtualTopicManager;
        this.fetchSessions = fetchSessions;
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
        if (!virtualTopicManager.sharedPhysicalTopics(data.topics().stream().map(FetchRequestData.FetchTopic::topic).toList()).isEmpty()) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), state.apiVersion(), refusal(data)));
            return;
        }
        for (FetchRequestData.FetchTopic topic : data.topics()) {
            String physical = virtualTopicManager.toPhysical(topic.topic());
            if (!Objects.equals(physical, topic.topic())) {
                state.record(physical, topic.topic());
                topic.setTopic(physical);
            }
            for (FetchRequestData.FetchPartition partition : topic.partitions()) {
                state.fetchOffset(new TopicPartition(physical, partition.partition()), partition.fetchOffset());
            }
        }
        List<TopicPartition> forgottenPartitions = new ArrayList<>();
        for (FetchRequestData.ForgottenTopic forgotten : data.forgottenTopicsData()) {
            forgotten.setTopic(virtualTopicManager.toPhysical(forgotten.topic()));
            for (int partition : forgotten.partitions()) {
                forgottenPartitions.add(new TopicPartition(forgotten.topic(), partition));
            }
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
                context.source(), data.sessionId(), state.physicalToVirtual(), forgottenPhysical, state.fetchOffsets(), forgottenPartitions
        );
    }

    @Override
    public void onResponse(
            GatewayContext context,
            FetchResponseData data
    ) {
        VirtualTopicState state = VirtualTopicState.from(context);
        int sessionId = data.sessionId();
        if (data.errorCode() == Errors.FETCH_SESSION_ID_NOT_FOUND.code()) {
            // The broker answers this error with session id 0: drop the session the request named.
            if (state.fetchSessionId() != 0) {
                fetchSessions.removeSession(context.source(), state.fetchSessionId());
            }
            if (sessionId != 0) {
                fetchSessions.removeSession(context.source(), sessionId);
            }
        }
        if (sessionId != 0 && state.fetchSessionId() == 0) {
            fetchSessions.bindSession(context.source(), sessionId, state.physicalToVirtual(), state.fetchOffsets());
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
            rewriteRecords(data, context, sessionId);
            return;
        }
        for (FetchResponseData.FetchableTopicResponse topic : data.responses()) {
            String virtual = state.virtualFor(topic.topic());
            if (virtual != null) {
                topic.setTopic(virtual);
            }
        }

        rewriteRecords(data, context, sessionId);
    }

    private void rewriteRecords(FetchResponseData data, GatewayContext context, int sessionId) {
        var virtualTopicState = VirtualTopicState.from(context);
        for (FetchResponseData.FetchableTopicResponse topic : data.responses()) {
            // Only a topic fetched by its virtual name is filtered and transformed: an exposed
            // physical topic delivers its records as they are stored.
            var virtualTopic = virtualTopicManager.virtualTopic(topic.topic())
                    .orElse(null);
            if (virtualTopic == null) {
                continue;
            }
            var virtualTopicRecordFilter = virtualTopic.recordFilter();
            var virtualTopicRecordTransformer = virtualTopic.transformer();
            if (virtualTopicRecordFilter == null && virtualTopicRecordTransformer == null) {
                continue;
            }
            String physical = virtualTopic.physical();
            for (FetchResponseData.PartitionData partition : topic.partitions()) {
                BaseRecords records = partition.records();
                try {
                    if (records instanceof MemoryRecords memoryRecords && memoryRecords.sizeInBytes() > 0) {
                        records = fromFetchOffset(memoryRecords, virtualTopicState, context.source(), sessionId,
                                new TopicPartition(physical, partition.partitionIndex()));
                    }
                    try {
                        records = virtualTopicRecordTransformer != null
                                ? virtualTopicRecordTransformer.apply(virtualTopicRecordFilter, records)
                                : virtualTopicRecordFilter.apply(records);
                    } catch (FailedRecordException stop) {
                        records = stop.recordsBefore();
                        if (records.sizeInBytes() == 0) {
                            log.warn("Failing fetch of {}-{} at offset {}: {}",
                                    topic.topic(), partition.partitionIndex(), stop.offset(), stop.getMessage());
                            partition.setErrorCode(Errors.CORRUPT_MESSAGE.code());
                        }
                    }
                } catch (SchemaRegistryUnavailableException e) {
                    log.warn("Failing fetch of {}-{}, the schema registry is unavailable: {}",
                            topic.topic(), partition.partitionIndex(), e.getMessage());
                    partition.setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code());
                    records = MemoryRecords.EMPTY;
                } catch (RuntimeException e) {
                    log.error("Failing fetch of {}-{}, its records could not be rewritten",
                            topic.topic(), partition.partitionIndex(), e);
                    partition.setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code());
                    records = MemoryRecords.EMPTY;
                }
                partition.setRecords(records);
            }
        }
    }

    /// The records of `partition` from the offset the client fetched it at - named in this request,
    /// else remembered by its fetch session - on: the broker returns whole batches, and the records
    /// below that offset the client discards. Evaluating them could stop the partition at a bad
    /// record the client already moved past, answering it records it ignores, forever.
    private MemoryRecords fromFetchOffset(MemoryRecords records, VirtualTopicState state, Object client, int sessionId, TopicPartition partition) {
        OptionalLong fetchOffset = state.fetchOffset(partition);
        if (fetchOffset.isEmpty()) {
            fetchOffset = fetchSessions.fetchOffset(client, sessionId, partition);
        }
        if (fetchOffset.isEmpty()) {
            log.debug("No fetch offset known for {}, rewriting all of its fetched records", partition);
            return records;
        }
        return RecordBatches.from(records, fetchOffset.getAsLong());
    }

    /// Answers every topic and partition the request named, under the client's names, with [Errors#INVALID_REQUEST].
    private static FetchResponseData refusal(FetchRequestData data) {
        var response = new FetchResponseData();
        for (FetchRequestData.FetchTopic topic : data.topics()) {
            var topicResponse = new FetchResponseData.FetchableTopicResponse().setTopic(topic.topic());
            for (FetchRequestData.FetchPartition partition : topic.partitions()) {
                topicResponse.partitions().add(new FetchResponseData.PartitionData()
                        .setPartitionIndex(partition.partition())
                        .setErrorCode(Errors.INVALID_REQUEST.code()));
            }
            response.responses().add(topicResponse);
        }
        return response;
    }
}

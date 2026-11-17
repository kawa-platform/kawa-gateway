package io.jonasg.kawa.virtualtopic.transform;

import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import org.apache.kafka.common.message.DescribeTransactionsRequestData;
import org.apache.kafka.common.message.DescribeTransactionsResponseData;

import java.util.ArrayList;
import java.util.List;

/// Rewrites the topics an in-flight transaction touches from physical to virtual names. The
/// request carries only transactional ids, so there is no per-request mapping to consult and
/// the rewrite falls back to the configured virtual-topic map.
public final class DescribeTransactionsVirtualTopicTransform
        implements VirtualTopicTransform<DescribeTransactionsRequestData, DescribeTransactionsResponseData> {

    private static final short DESCRIBE_TRANSACTIONS = 65;

    private final VirtualTopicManager virtualTopics;

    public DescribeTransactionsVirtualTopicTransform(VirtualTopicManager virtualTopics) {
        this.virtualTopics = virtualTopics;
    }

    @Override
    public short apiKey() {
        return DESCRIBE_TRANSACTIONS;
    }

    @Override
    public void onRequest(
            GatewayContext context,
            DescribeTransactionsRequestData data
    ) {
    }

    @Override
    public void onResponse(
            GatewayContext context,
            DescribeTransactionsResponseData data
    ) {
        for (DescribeTransactionsResponseData.TransactionState state : data.transactionStates()) {
            // A physical topic with several virtual topics is listed once per virtual topic: the first takes over
            // this entry and every other one is appended to state.topics(). Iterate a copy, since appending to the
            // list being iterated throws ConcurrentModificationException.
            for (DescribeTransactionsResponseData.TopicData topic : new ArrayList<>(state.topics())) {
                List<String> names = virtualTopics.toVirtual(topic.topic());
                for (String name : names.subList(1, names.size())) {
                    state.topics().add(topic.duplicate().setTopic(name));
                }
                topic.setTopic(names.getFirst());
            }
        }
    }
}

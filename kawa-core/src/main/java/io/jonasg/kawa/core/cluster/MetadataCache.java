package io.jonasg.kawa.core.cluster;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/// Thread-safe cache of the physical cluster topology, populated from decoded broker
/// Metadata responses. Used by the router to pick the leader broker for a partition.
public final class MetadataCache {

    private final Map<String, TopicMetadata> topics = new ConcurrentHashMap<>();
    private final Map<Integer, BrokerNode> brokers = new ConcurrentHashMap<>();
    private volatile String clusterId;

    public void update(MetadataSnapshot snapshot) {
        topics.clear();
        topics.putAll(snapshot.topics());
        brokers.clear();
        brokers.putAll(snapshot.brokers());
        clusterId = snapshot.clusterId();
    }

    /// Broker id leading `partition` of `physicalTopic`, or `-1` if unknown.
    public int leaderFor(
            String physicalTopic,
            int partition
    ) {
        TopicMetadata topic = topics.get(physicalTopic);
        if (topic == null) {
            return -1;
        }
        for (PartitionMetadata metadata : topic.partitions()) {
            if (metadata.index() == partition) {
                return metadata.leaderId();
            }
        }
        return -1;
    }

    /// Broker id leading any partition of `physicalTopic`, or `-1` if unknown.
    public int leaderForAny(String physicalTopic) {
        TopicMetadata topic = topics.get(physicalTopic);
        if (topic == null || topic.partitions().isEmpty()) {
            return -1;
        }
        return topic.partitions().getFirst().leaderId();
    }

    public BrokerNode broker(int brokerId) {
        return brokers.get(brokerId);
    }

    /// Whether the cache knows `physicalTopic`.
    public boolean contains(String physicalTopic) {
        return topics.containsKey(physicalTopic);
    }

    /// Number of partitions for `physicalTopic`, or `0` if unknown.
    public int partitionCount(String physicalTopic) {
        TopicMetadata topic = topics.get(physicalTopic);
        return topic == null ? 0 : topic.partitions().size();
    }

    /// Replication factor (replica count per partition) for `physicalTopic`, or `0` if unknown.
    public int replicationFactor(String physicalTopic) {
        TopicMetadata topic = topics.get(physicalTopic);
        if (topic == null || topic.partitions().isEmpty()) {
            return 0;
        }
        return topic.partitions().getFirst().replicas().size();
    }

    public Collection<BrokerNode> brokers() {
        return brokers.values();
    }

    public Collection<TopicMetadata> topics() {
        return topics.values();
    }
}

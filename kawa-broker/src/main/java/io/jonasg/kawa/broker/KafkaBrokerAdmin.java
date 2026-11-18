package io.jonasg.kawa.broker;

import io.jonasg.kawa.governance.TopicDescriber.TopicState;
import io.jonasg.kawa.governance.TopicSpec;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DescribeConfigsOptions;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.admin.ListGroupsOptions;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

/// [BrokerAdmin] backed by Kafka's [AdminClient]: creates, deletes and describes topics on the
/// real cluster using the same bootstrap servers and broker credentials as the gateway. Also
/// the gateway's describer for judging changes on the Kafka
/// protocol: describing asks the broker on every call, since changes are rare admin operations.
/// Consumer group reads are batched: one AdminClient call covers every group asked for.
public final class KafkaBrokerAdmin implements BrokerAdmin {

    private static final int DESCRIBE_TIMEOUT_MS = 5_000;

    private final AdminClient admin;

    /// @param brokerSecurity the Kafka client settings for reaching the brokers (`security.protocol`,
    ///                       `sasl.*`, `ssl.*`), see `BrokerSecurity.clientProperties`
    public KafkaBrokerAdmin(String bootstrapServers, Properties brokerSecurity) {
        this(AdminClient.create(props(bootstrapServers, brokerSecurity)));
    }

    /// Package-private for tests.
    KafkaBrokerAdmin(AdminClient admin) {
        this.admin = admin;
    }

    @Override
    public void createTopic(TopicSpec spec) throws Exception {
        var topic = new NewTopic(spec.name(), spec.partitions(), (short) spec.replicationFactor())
                .configs(spec.configs());
        admin.createTopics(List.of(topic)).all().get();
    }

    @Override
    public void deleteTopic(String name) throws Exception {
        admin.deleteTopics(List.of(name)).all().get();
    }

    @Override
    public CompletionStage<Map<String, TopicState>> describe(Collection<String> topics) {
        var descriptions = admin.describeTopics(topics, new DescribeTopicsOptions().timeoutMs(DESCRIBE_TIMEOUT_MS)).topicNameValues();
        var configs = admin.describeConfigs(
                topics.stream().map(name -> new ConfigResource(ConfigResource.Type.TOPIC, name)).toList(),
                new DescribeConfigsOptions().timeoutMs(DESCRIBE_TIMEOUT_MS)).values();
        Map<String, CompletableFuture<Optional<TopicState>>> states = new HashMap<>();
        topics.forEach(name -> states.put(name, descriptions.get(name).toCompletionStage().toCompletableFuture()
                .thenCombine(configs.get(new ConfigResource(ConfigResource.Type.TOPIC, name)).toCompletionStage(),
                        (description, config) -> Optional.of(state(description, config)))
                .exceptionally(error -> absentWhen(UnknownTopicOrPartitionException.class, error))));
        return gather(states);
    }

    private static TopicState state(TopicDescription description, Config config) {
        Map<String, String> overrides = new HashMap<>();
        for (ConfigEntry entry : config.entries()) {
            if (entry.source() == ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG && entry.value() != null) {
                overrides.put(entry.name(), entry.value());
            }
        }
        int replicationFactor = description.partitions().isEmpty() ? -1 : description.partitions().getFirst().replicas().size();
        return new TopicState(description.partitions().size(), replicationFactor, overrides);
    }

    @Override
    public CompletionStage<List<GroupSummary>> listGroups() {
        return admin.listGroups(new ListGroupsOptions().timeoutMs(DESCRIBE_TIMEOUT_MS)).all().toCompletionStage()
                .thenApply(listings -> listings.stream()
                        .map(listing -> new GroupSummary(
                                listing.groupId(),
                                listing.type().orElse(GroupType.UNKNOWN).name(),
                                listing.groupState().orElse(GroupState.UNKNOWN).toString(),
                                listing.protocol().isEmpty() ? null : listing.protocol()))
                        .toList());
    }

    @Override
    public CompletionStage<Map<String, GroupDescription>> describeConsumerGroups(Collection<String> groupIds) {
        var futures = admin.describeConsumerGroups(groupIds, new DescribeConsumerGroupsOptions().timeoutMs(DESCRIBE_TIMEOUT_MS))
                .describedGroups();
        Map<String, CompletableFuture<Optional<GroupDescription>>> groups = new HashMap<>();
        futures.forEach((groupId, future) -> groups.put(groupId, future.toCompletionStage().toCompletableFuture()
                .thenApply(description -> Optional.of(toGroupDescription(description)))
                .exceptionally(error -> absentWhen(GroupIdNotFoundException.class, error))));
        return gather(groups);
    }

    private static GroupDescription toGroupDescription(ConsumerGroupDescription description) {
        var members = description.members().stream()
                .map(member -> new GroupMember(
                        member.consumerId(),
                        member.clientId(),
                        member.host(),
                        member.assignment().topicPartitions().stream().map(KafkaBrokerAdmin::partitionId).toList()))
                .toList();
        return new GroupDescription(description.groupId(), description.type().name(), description.groupState().toString(), members);
    }

    @Override
    public CompletionStage<Map<String, Map<PartitionId, Long>>> committedOffsets(Collection<String> groupIds) {
        var specs = new HashMap<String, ListConsumerGroupOffsetsSpec>();
        groupIds.forEach(groupId -> specs.put(groupId, new ListConsumerGroupOffsetsSpec()));
        var result = admin.listConsumerGroupOffsets(specs, new ListConsumerGroupOffsetsOptions().timeoutMs(DESCRIBE_TIMEOUT_MS));
        Map<String, CompletableFuture<Optional<Map<PartitionId, Long>>>> committed = new HashMap<>();
        groupIds.forEach(groupId -> committed.put(groupId, result.partitionsToOffsetAndMetadata(groupId).toCompletionStage()
                .toCompletableFuture()
                .thenApply(offsets -> Optional.of(toPartitionOffsets(offsets)))
                .exceptionally(error -> absentWhen(GroupIdNotFoundException.class, error))));
        return gather(committed);
    }

    private static Map<PartitionId, Long> toPartitionOffsets(Map<TopicPartition, OffsetAndMetadata> offsets) {
        Map<PartitionId, Long> byPartition = new HashMap<>();
        offsets.forEach((partition, committed) -> {
            if (committed != null) {
                byPartition.put(partitionId(partition), committed.offset());
            }
        });
        return byPartition;
    }

    @Override
    public CompletionStage<Map<PartitionId, Long>> latestOffsets(Collection<PartitionId> partitions) {
        Map<TopicPartition, OffsetSpec> specs = new HashMap<>();
        partitions.forEach(partition -> specs.put(topicPartition(partition), OffsetSpec.latest()));
        var result = admin.listOffsets(specs, new ListOffsetsOptions(IsolationLevel.READ_COMMITTED).timeoutMs(DESCRIBE_TIMEOUT_MS));
        Map<PartitionId, CompletableFuture<Optional<Long>>> latest = new HashMap<>();
        partitions.forEach(partition -> latest.put(partition, result.partitionResult(topicPartition(partition))
                .toCompletionStage().toCompletableFuture()
                .thenApply(info -> Optional.of(info.offset()))
                .exceptionally(error -> absentWhen(UnknownTopicOrPartitionException.class, error))));
        return gather(latest);
    }

    /// Waits for every entry and keeps the present ones; an absent entry is left out.
    private static <K, V> CompletionStage<Map<K, V>> gather(Map<K, CompletableFuture<Optional<V>>> entries) {
        return CompletableFuture.allOf(entries.values().toArray(CompletableFuture[]::new)).thenApply(_ -> {
            Map<K, V> found = new HashMap<>();
            entries.forEach((key, entry) -> entry.join().ifPresent(value -> found.put(key, value)));
            return found;
        });
    }

    /// The empty result when the broker reports the entity as absent (`absent`); any other failure fails the lookup.
    private static <T> Optional<T> absentWhen(Class<? extends Throwable> absent, Throwable error) {
        Throwable cause = error;
        while ((cause instanceof CompletionException || cause instanceof ExecutionException) && cause.getCause() != null) {
            cause = cause.getCause();
        }
        if (absent.isInstance(cause)) {
            return Optional.empty();
        }
        throw error instanceof CompletionException completion ? completion : new CompletionException(error);
    }

    private static PartitionId partitionId(TopicPartition partition) {
        return new PartitionId(partition.topic(), partition.partition());
    }

    private static TopicPartition topicPartition(PartitionId partition) {
        return new TopicPartition(partition.topic(), partition.partition());
    }

    @Override
    public void close() {
        admin.close();
    }

    /// AdminClient properties: the bootstrap servers plus the broker security settings.
    static Properties props(String bootstrapServers, Properties brokerSecurity) {
        Properties props = new Properties();
        props.putAll(brokerSecurity);
        props.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return props;
    }
}

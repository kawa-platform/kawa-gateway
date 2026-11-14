package io.jonasg.kawa.http.topics;

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
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import java.util.Collection;
import java.util.Comparator;
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

/// [TopicAdmin] backed by Kafka's [AdminClient]: creates, deletes and describes topics, and reads
/// consumer groups and their offsets, on the
/// real cluster using the same bootstrap servers and broker credentials as the gateway. Also
/// the gateway's describer for judging changes on the Kafka
/// protocol: describing asks the broker on every call, since changes are rare admin operations.
public final class KafkaTopicAdmin implements TopicAdmin {

    private static final int DESCRIBE_TIMEOUT_MS = 5_000;

    private final AdminClient admin;

    /// @param brokerSecurity the Kafka client settings for reaching the brokers (`security.protocol`,
    ///                       `sasl.*`, `ssl.*`), see `BrokerSecurity.clientProperties`
    public KafkaTopicAdmin(String bootstrapServers, Properties brokerSecurity) {
        this(AdminClient.create(props(bootstrapServers, brokerSecurity)));
    }

    /// Package-private for tests.
    KafkaTopicAdmin(AdminClient admin) {
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
    public List<GroupSummary> listConsumerGroups() throws Exception {
        return admin.listGroups(ListGroupsOptions.forConsumerGroups().timeoutMs(DESCRIBE_TIMEOUT_MS)).all().get().stream()
                .map(group -> new GroupSummary(group.groupId(), group.groupState().map(GroupState::toString)
                        .orElse(GroupState.UNKNOWN.toString())))
                .toList();
    }

    @Override
    public Map<String, GroupDetails> describeConsumerGroups(Collection<String> groupIds) throws Exception {
        var described = admin.describeConsumerGroups(groupIds, new DescribeConsumerGroupsOptions().timeoutMs(DESCRIBE_TIMEOUT_MS))
                .describedGroups();
        Map<String, GroupDetails> byId = new HashMap<>();
        for (var entry : described.entrySet()) {
            ConsumerGroupDescription group;
            try {
                group = entry.getValue().get();
            } catch (ExecutionException e) {
                if (e.getCause() instanceof GroupIdNotFoundException) {
                    continue;
                }
                throw e;
            }
            // A classic group the coordinator does not know is described as DEAD rather than refused.
            if (group.groupState() != GroupState.DEAD) {
                byId.put(entry.getKey(), new GroupDetails(group.groupId(), group.groupState().toString(),
                        group.members().stream().map(KafkaTopicAdmin::member).toList()));
            }
        }
        return byId;
    }

    @Override
    public Map<String, Map<GroupPartition, Long>> committedOffsets(Collection<String> groupIds) throws Exception {
        if (groupIds.isEmpty()) {
            return Map.of();
        }
        Map<String, ListConsumerGroupOffsetsSpec> specs = new HashMap<>();
        groupIds.forEach(id -> specs.put(id, new ListConsumerGroupOffsetsSpec()));
        Map<String, Map<TopicPartition, OffsetAndMetadata>> offsets = admin.listConsumerGroupOffsets(specs,
                new ListConsumerGroupOffsetsOptions().timeoutMs(DESCRIBE_TIMEOUT_MS)).all().get();
        Map<String, Map<GroupPartition, Long>> byGroup = new HashMap<>();
        offsets.forEach((groupId, committed) -> {
            Map<GroupPartition, Long> partitions = new HashMap<>();
            committed.forEach((partition, offset) -> {
                if (offset != null) {
                    partitions.put(partition(partition), offset.offset());
                }
            });
            byGroup.put(groupId, partitions);
        });
        return byGroup;
    }

    @Override
    public Map<GroupPartition, Long> endOffsets(Collection<GroupPartition> partitions) throws Exception {
        if (partitions.isEmpty()) {
            return Map.of();
        }
        Map<TopicPartition, OffsetSpec> specs = new HashMap<>();
        partitions.forEach(p -> specs.put(new TopicPartition(p.topic(), p.partition()), OffsetSpec.latest()));
        var result = admin.listOffsets(specs, new ListOffsetsOptions().timeoutMs(DESCRIBE_TIMEOUT_MS));
        Map<GroupPartition, Long> byPartition = new HashMap<>();
        for (TopicPartition partition : specs.keySet()) {
            KafkaFuture<ListOffsetsResultInfo> future = result.partitionResult(partition);
            try {
                byPartition.put(partition(partition), future.get().offset());
            } catch (ExecutionException e) {
                // a partition of a deleted topic is left out; any other failure fails the lookup
                if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) {
                    throw e;
                }
            }
        }
        return byPartition;
    }

    private static GroupMember member(MemberDescription member) {
        var assignment = member.assignment().topicPartitions().stream()
                .map(KafkaTopicAdmin::partition)
                .sorted(Comparator.comparing(GroupPartition::topic).thenComparingInt(GroupPartition::partition))
                .toList();
        return new GroupMember(member.consumerId(), member.clientId(), member.host(), assignment);
    }

    private static GroupPartition partition(TopicPartition partition) {
        return new GroupPartition(partition.topic(), partition.partition());
    }

    @Override
    public CompletionStage<Map<String, TopicState>> describe(Collection<String> topics) {
        var descriptions = admin.describeTopics(topics, new DescribeTopicsOptions().timeoutMs(DESCRIBE_TIMEOUT_MS)).topicNameValues();
        var configs = admin.describeConfigs(
                topics.stream().map(name -> new ConfigResource(ConfigResource.Type.TOPIC, name)).toList(),
                new DescribeConfigsOptions().timeoutMs(DESCRIBE_TIMEOUT_MS)).values();
        List<CompletableFuture<Optional<Map.Entry<String, TopicState>>>> states = topics.stream()
                .map(name -> descriptions.get(name).toCompletionStage().toCompletableFuture()
                        .thenCombine(configs.get(new ConfigResource(ConfigResource.Type.TOPIC, name)).toCompletionStage(),
                                (description, config) -> Optional.of(Map.entry(name, state(description, config))))
                        .exceptionally(KafkaTopicAdmin::unknownTopic))
                .toList();
        return CompletableFuture.allOf(states.toArray(CompletableFuture[]::new)).thenApply(_ -> {
            Map<String, TopicState> byName = new HashMap<>();
            states.forEach(state -> state.join().ifPresent(e -> byName.put(e.getKey(), e.getValue())));
            return byName;
        });
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

    /// A topic the broker does not know is left out; any other failure fails the lookup.
    private static Optional<Map.Entry<String, TopicState>> unknownTopic(Throwable error) {
        Throwable cause = error;
        while ((cause instanceof CompletionException || cause instanceof ExecutionException) && cause.getCause() != null) {
            cause = cause.getCause();
        }
        if (cause instanceof UnknownTopicOrPartitionException) {
            return Optional.empty();
        }
        throw error instanceof CompletionException completion ? completion : new CompletionException(error);
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

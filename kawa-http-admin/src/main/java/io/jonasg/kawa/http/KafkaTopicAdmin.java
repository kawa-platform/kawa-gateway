package io.jonasg.kawa.http;

import io.jonasg.kawa.config.BrokerAuthConfig;
import io.jonasg.kawa.governance.TopicDescriber.TopicState;
import io.jonasg.kawa.governance.TopicSpec;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.SaslConfigs;
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
import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;

/// [TopicAdmin] backed by Kafka's [AdminClient]: creates, deletes and describes topics on the
/// real cluster using the same bootstrap servers and broker credentials as the gateway. Also
/// the gateway's describer for judging changes on the Kafka
/// protocol: describing asks the broker on every call, since changes are rare admin operations.
public final class KafkaTopicAdmin implements TopicAdmin {

    private static final int DESCRIBE_TIMEOUT_MS = 5_000;

    private final AdminClient admin;

    public KafkaTopicAdmin(String bootstrapServers, BrokerAuthConfig brokerAuth) {
        this(AdminClient.create(props(bootstrapServers, brokerAuth)));
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

    /// AdminClient properties: bootstrap servers plus the gateway's broker SASL credentials
    /// when configured.
    static Properties props(String bootstrapServers, BrokerAuthConfig brokerAuth) {
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        if (brokerAuth != null) {
            props.put(SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
            props.put(SaslConfigs.SASL_MECHANISM, brokerAuth.mechanism());
            props.put(SaslConfigs.SASL_JAAS_CONFIG,
                    "org.apache.kafka.common.security.plain.PlainLoginModule required "
                    + "username=\"" + brokerAuth.username() + "\" password=\"" + brokerAuth.password() + "\";");
        }
        return props;
    }
}

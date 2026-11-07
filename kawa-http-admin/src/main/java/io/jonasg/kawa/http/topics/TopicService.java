package io.jonasg.kawa.http.topics;

import io.jonasg.kawa.broker.TopicAdmin;
import io.jonasg.kawa.config.DynamicConfig;
import io.jonasg.kawa.config.DynamicConfigRepository;
import io.jonasg.kawa.config.ManagedBy;
import io.jonasg.kawa.config.GovernanceRuleConfig.Operation;
import io.jonasg.kawa.config.PayloadFormatConfig;
import io.jonasg.kawa.config.VirtualTopicConfig;
import io.jonasg.kawa.config.VirtualTopicFilterConfig;
import io.jonasg.kawa.core.cluster.MetadataCache;
import io.jonasg.kawa.core.cluster.TopicMetadata;
import io.jonasg.kawa.governance.GovernancePolicy;
import io.jonasg.kawa.governance.GovernanceRequest;
import io.jonasg.kawa.governance.TopicDescriber.TopicState;
import io.jonasg.kawa.governance.TopicSpec;
import io.jonasg.kawa.governance.Violation;
import io.jonasg.kawa.http.configchange.ConflictException;
import io.jonasg.kawa.http.configchange.Consistency;
import io.jonasg.kawa.http.configchange.ConsistencyAwareUpdater;
import io.jonasg.kawa.http.configchange.ForbiddenException;
import io.jonasg.kawa.http.configchange.NotFoundException;
import io.jonasg.kawa.http.configchange.Ownership;
import io.jonasg.kawa.plugin.auth.KawaPrincipal;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/// Owns the admin topic surface: virtual topic config CRUD, physical topic listing,
/// broker create/delete, and governance admission. Governance judges each change as the
/// authenticated caller (`principal`): an admin user's email, a service client's id, or the
/// principal a delegate acts for (e.g. `k8s:team-a`). Every virtual topic write honours
/// [Ownership]: a managed virtual topic only changes through its owner. Owns the [TopicAdmin]:
/// closing the service closes it.
public final class TopicService implements AutoCloseable {

    private static final String KIND = "topic";
    private static final String SERVICE = "kawa";
    private static final int DESCRIBE_TIMEOUT_SECONDS = 10;

    private final VirtualTopicManager virtualTopicManager;
    private final MetadataCache cache;
    private final DynamicConfigRepository dynamicConfigRepository;
    private final TopicAdmin topicAdmin;
    private final GovernancePolicy governancePolicy;
    private final ConsistencyAwareUpdater consistencyAwareUpdater;

    public TopicService(
            VirtualTopicManager virtualTopicManager,
            MetadataCache cache,
            DynamicConfigRepository dynamicConfigRepository,
            TopicAdmin topicAdmin,
            GovernancePolicy governancePolicy
    ) {
        this.virtualTopicManager = virtualTopicManager;
        this.cache = cache;
        this.dynamicConfigRepository = dynamicConfigRepository;
        this.topicAdmin = topicAdmin;
        this.governancePolicy = governancePolicy;
        this.consistencyAwareUpdater = new ConsistencyAwareUpdater(dynamicConfigRepository);
    }

    @Override
    public void close() {
        topicAdmin.close();
    }

    /// The virtual topics the [VirtualTopicManager] serves and the physical topics in the broker
    /// metadata cache.
    public TopicListing listTopics() {
        Map<String, VirtualTopicConfig> configured = dynamicConfigRepository.getActiveConfigOrEmpty().virtualTopics();
        List<TopicDescription.VirtualTopicEntry> virtual = new ArrayList<>();
        for (Map.Entry<String, String> entry : virtualTopicManager.virtualTopics().entrySet()) {
            String name = entry.getKey();
            virtual.add(new TopicDescription.VirtualTopicEntry(
                    name,
                    entry.getValue(),
                    virtualTopicManager.filterFor(name).orElse(null),
                    virtualTopicManager.exposesPhysicalTopic(name),
                    virtualTopicManager.valueFormatFor(name).orElse(null),
                    managedBy(configured.get(name))));
        }
        return new TopicListing(virtual, List.copyOf(cache.topics()));
    }

    public TopicDescription describeTopic(String name) {
        try {
            var maybeVirtualTopic = this.virtualTopicManager.findByName(name);
            if (maybeVirtualTopic.isPresent()) {
                var virtualTopic = maybeVirtualTopic.get();
                String physical = virtualTopic.physical();
                var names = physical.equals(name) ? List.of(name) : List.of(physical, name);
                Map<String, TopicState> topics = this.topicAdmin.describe(names).toCompletableFuture().get();
                var state = topics.get(physical);
                return new TopicDescription(
                        name,
                        virtualTopic,
                        physical,
                        state == null ? 0 : state.partitions(),
                        state == null ? 0 : state.replicationFactor(),
                        state == null ? Map.of() : state.configs(),
                        !physical.equals(name) && topics.containsKey(name)
                );
            }

            var res = this.topicAdmin.describe(List.of(name));
            Map<String, TopicState> topics = res.toCompletableFuture().get();
            if (topics.containsKey(name)) {
                var state = topics.get(name);
                return new TopicDescription(
                        name,
                        null,
                        name,
                        state.partitions(),
                        state.replicationFactor(),
                        state.configs(),
                        false
                );
            }

            throw new TopicNotFoundException("Unable to describe an non existing topic: %s".formatted(name));
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    VirtualTopicConfig upsertVirtualTopic(KawaPrincipal caller, String name, TopicRequest request, Consistency consistency) {
        String principal = principalName(caller);
        if (!"virtual".equals(request.type())) {
            throw new IllegalArgumentException("only 'virtual' topics can be updated");
        }
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("topic name is required");
        }
        if (request.topic() == null || request.topic().isBlank()) {
            throw new IllegalArgumentException("virtual topic requires a physical 'topic'");
        }
        var operation = dynamicConfigRepository.getActiveConfigOrEmpty().virtualTopics().containsKey(name) ? Operation.ALTER : Operation.CREATE;
        admit(name, GovernanceRequest.virtualTopic(principal, SERVICE, operation, name, request.topic()));
        var value = new VirtualTopicConfig(
                request.topic(),
                request.filter(),
                request.exposePhysicalTopic() != null && request.exposePhysicalTopic(),
                request.valueFormat(),
                Ownership.stamp(caller));
        consistencyAwareUpdater.update(consistency, config -> {
            requireTopicOwner(config, name, caller);
            return config.upsertVirtualTopic(name, value);
        });
        return value;
    }

    VirtualTopicConfig updateVirtualTopic(KawaPrincipal caller, String name, VirtualTopicConfigPatch patch, Consistency consistency) {
        String principal = principalName(caller);
        DynamicConfig base = dynamicConfigRepository.getActiveConfigOrEmpty();
        VirtualTopicConfig current = base.virtualTopics().get(name);
        if (current == null) {
            if (cache.topics().stream().anyMatch(topic -> topic.name().equals(name))) {
                throw new IllegalArgumentException("only virtual topics can be patched");
            }
            throw new NotFoundException("topic '" + name + "' not found");
        }

        String newName = patch.name() == null ? name : patch.name();
        if (newName.isBlank()) {
            throw new IllegalArgumentException("topic name must not be blank");
        }
        if (!newName.equals(name) && base.virtualTopics().containsKey(newName)) {
            throw new ConflictException("topic '" + newName + "' already exists");
        }

        String topic = patch.topic() == null ? current.topic() : patch.topic();
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("virtual topic requires a physical 'topic'");
        }
        admit(newName, GovernanceRequest.virtualTopic(principal, SERVICE, Operation.ALTER, newName, topic));
        var updated = new VirtualTopicConfig(
                topic,
                patch.filter(),
                patch.exposePhysicalTopic() == null ? current.exposePhysicalTopic() : patch.exposePhysicalTopic(),
                patch.valueFormat(),
                Ownership.stamp(caller));
        consistencyAwareUpdater.update(consistency, config -> {
            requireTopicOwner(config, name, caller);
            return replace(config, name, newName, updated);
        });
        return updated;
    }

    void deleteTopic(KawaPrincipal caller, String name, Consistency consistency) {
        String principal = principalName(caller);
        DynamicConfig base = dynamicConfigRepository.getActiveConfigOrEmpty();
        VirtualTopicConfig virtual = base.virtualTopics().get(name);
        if (virtual != null) {
            admit(name, GovernanceRequest.virtualTopic(principal, SERVICE, Operation.DELETE, name, virtual.topic()));
            consistencyAwareUpdater.update(consistency, config -> {
                requireTopicOwner(config, name, caller);
                return config.removeVirtualTopic(name);
            });
            return;
        }
        boolean physicalExists = cache.topics().stream().anyMatch(t -> t.name().equals(name));
        if (!physicalExists) {
            throw new NotFoundException("topic '" + name + "' not found");
        }
        if (governancePolicy.hasPhysicalTopicRules(Operation.DELETE)) {
            TopicState current = currentState(name);
            if (current == null) {
                throw new NotFoundException("topic '" + name + "' not found");
            }
            admit(name, GovernanceRequest.topicDelete(principal, SERVICE,
                    new TopicSpec(name, current.partitions(), current.replicationFactor(), current.configs())));
        }
        try {
            topicAdmin.deleteTopic(name);
        } catch (Exception e) {
            throw new IllegalStateException("failed to delete topic '" + name + "': " + e.getMessage(), e);
        }
    }

    /// Clears the virtual topic's owner. Only the owner may release it; an unmanaged virtual topic
    /// is left as is.
    void releaseVirtualTopic(KawaPrincipal caller, String name, Consistency consistency) {
        VirtualTopicConfig current = dynamicConfigRepository.getActiveConfigOrEmpty().virtualTopics().get(name);
        if (current == null) {
            throw new NotFoundException("topic '" + name + "' not found");
        }
        if (current.managedBy() == null) {
            return;
        }
        consistencyAwareUpdater.update(consistency, config -> {
            VirtualTopicConfig topic = config.virtualTopics().get(name);
            if (topic == null) {
                throw new NotFoundException("topic '" + name + "' not found");
            }
            Ownership.requireOwner(KIND, name, topic.managedBy(), caller);
            return config.upsertVirtualTopic(name, topic.withManagedBy(null));
        });
    }

    TopicSpec createPhysicalTopic(KawaPrincipal caller, TopicRequest request) {
        String principal = principalName(caller);
        if (!"physical".equals(request.type())) {
            throw new IllegalArgumentException("type must be 'physical' or 'virtual'");
        }
        if (request.name() == null || request.name().isBlank()) {
            throw new IllegalArgumentException("topic name is required");
        }
        var spec = new TopicSpec(
                request.name(),
                request.partitions() == null ? -1 : request.partitions(),
                request.replicationFactor() == null ? -1 : request.replicationFactor(),
                request.configs());
        admit(spec.name(), GovernanceRequest.topic(principal, SERVICE, spec));
        try {
            topicAdmin.createTopic(spec);
        } catch (Exception e) {
            if (isTopicExists(e)) {
                throw new ConflictException("topic '" + spec.name() + "' already exists");
            }
            throw new IllegalStateException("failed to create topic '" + spec.name() + "': " + e.getMessage(), e);
        }
        return spec;
    }

    /// The topic's current state from the broker, or `null` when the broker does not know it.
    private TopicState currentState(String name) {
        try {
            return topicAdmin.describe(List.of(name)).toCompletableFuture()
                    .get(DESCRIBE_TIMEOUT_SECONDS, TimeUnit.SECONDS).get(name);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted reading topic '" + name + "'", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("governance could not read the current state of topic '" + name + "': "
                                            + (e.getCause() == null ? e.getMessage() : e.getCause().getMessage()), e);
        }
    }

    /// Refuses the request with `403` and every violation when governance does not allow it.
    private void admit(String name, GovernanceRequest request) {
        List<Violation> violations = governancePolicy.evaluate(request);
        if (!violations.isEmpty()) {
            String detail = violations.stream()
                    .map(Violation::describe)
                    .collect(Collectors.joining("; "));
            throw new ForbiddenException("topic '" + name + "' rejected by governance: " + detail);
        }
    }

    /// The number of partitions of `physicalTopic` in the broker metadata cache, or `0` if unknown.
    public int partitionCount(String physicalTopic) {
        return cache.partitionCount(physicalTopic);
    }

    /// The replica count per partition of `physicalTopic` in the broker metadata cache, or `0` if
    /// unknown.
    public int replicationFactor(String physicalTopic) {
        return cache.replicationFactor(physicalTopic);
    }

    private static DynamicConfig replace(
            DynamicConfig config,
            String currentName,
            String newName,
            VirtualTopicConfig updated
    ) {
        var topics = new HashMap<>(config.virtualTopics());
        topics.remove(currentName);
        topics.put(newName, updated);
        return config.updateVirtualTopics(Map.copyOf(topics));
    }

    private static void requireTopicOwner(DynamicConfig config, String name, KawaPrincipal caller) {
        Ownership.requireOwner(KIND, name, managedBy(config.virtualTopics().get(name)), caller);
    }

    private static @Nullable ManagedBy managedBy(@Nullable VirtualTopicConfig topic) {
        return topic == null ? null : topic.managedBy();
    }

    private static @Nullable String principalName(@Nullable KawaPrincipal caller) {
        return caller == null ? null : caller.name();
    }

    private static boolean isTopicExists(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof org.apache.kafka.common.errors.TopicExistsException) {
                return true;
            }
        }
        return false;
    }

    /// Raw virtual and physical topic data returned by [#listTopics()].
    ///
    /// @param virtual  the virtual topics
    /// @param physical the topics the broker metadata cache knows
    public record TopicListing(List<TopicDescription.VirtualTopicEntry> virtual, List<TopicMetadata> physical) {
    }

    /// One topic as the admin API describes it.
    ///
    /// @param name                 the topic name that was asked for
    /// @param virtualTopic         the virtual topic of that name, or `null` when the name is a physical topic
    /// @param physicalTopic        the broker topic that holds the records: the virtual topic's target, or the
    ///                             topic itself
    /// @param partitions           its partition count, `0` when the broker does not know it
    /// @param replicationFactor    its replica count per partition, `0` when unknown
    /// @param configs              the configs set on the topic itself
    /// @param shadowsPhysicalTopic whether a physical topic with the requested name also exists on the broker,
    ///                             hidden from clients by the virtual topic of that name
    public record TopicDescription(
            String name,
            VirtualTopicManager.@Nullable VirtualTopic virtualTopic,
            String physicalTopic,
            int partitions,
            int replicationFactor,
            Map<String, String> configs,
            boolean shadowsPhysicalTopic
    ) {

        /// One virtual topic as known by the [VirtualTopicManager].
        ///
        /// @param name                the virtual topic's name
        /// @param physicalTopic       the broker topic it maps to
        /// @param filter              the consume filter, or `null` when every record is served
        /// @param exposePhysicalTopic whether the physical topic is also served under its own name
        /// @param valueFormat         how record values are encoded, or `null` when they are not decoded
        public record VirtualTopicEntry(
                String name,
                String physicalTopic,
                @Nullable VirtualTopicFilterConfig filter,
                boolean exposePhysicalTopic,
                @Nullable PayloadFormatConfig valueFormat,
                @Nullable ManagedBy managedBy
        ) {
        }
    }
}

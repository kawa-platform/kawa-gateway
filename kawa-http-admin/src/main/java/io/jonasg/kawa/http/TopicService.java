package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.GatewayConfigRepository;
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
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/// Owns the admin topic surface: virtual topic config CRUD, physical topic listing,
/// broker create/delete, and governance admission.
final class TopicService {

    private static final String PRINCIPAL = "admin";
    private static final String SERVICE = "kawa";
    private static final int DESCRIBE_TIMEOUT_SECONDS = 10;

    private final VirtualTopicManager virtualTopics;
    private final MetadataCache cache;
    private final GatewayConfigRepository repository;
    private final TopicAdmin topicAdmin;
    private final GovernancePolicy governance;
    private final ConsistencyAwareUpdater updater;

    TopicService(
            VirtualTopicManager virtualTopics,
            MetadataCache cache,
            GatewayConfigRepository repository,
            TopicAdmin topicAdmin,
            GovernancePolicy governance
    ) {
        this.virtualTopics = virtualTopics;
        this.cache = cache;
        this.repository = repository;
        this.topicAdmin = topicAdmin;
        this.governance = governance;
        this.updater = new ConsistencyAwareUpdater(repository);
    }

    TopicListing listTopics() {
        List<VirtualTopicEntry> virtual = new ArrayList<>();
        for (Map.Entry<String, String> entry : virtualTopics.virtualTopics().entrySet()) {
            String name = entry.getKey();
            virtual.add(new VirtualTopicEntry(
                    name,
                    entry.getValue(),
                    virtualTopics.filterFor(name).orElse(null),
                    virtualTopics.exposesPhysicalTopic(name),
                    virtualTopics.valueFormatFor(name).orElse(null)));
        }
        return new TopicListing(virtual, List.copyOf(cache.topics()));
    }

    VirtualTopicConfig upsertVirtualTopic(String name, TopicRequest request, Consistency consistency) {
        if (!"virtual".equals(request.type())) {
            throw new IllegalArgumentException("only 'virtual' topics can be updated");
        }
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("topic name is required");
        }
        if (request.topic() == null || request.topic().isBlank()) {
            throw new IllegalArgumentException("virtual topic requires a physical 'topic'");
        }
        var operation = repository.getActiveConfigOrEmpty().virtualTopics().containsKey(name) ? Operation.ALTER : Operation.CREATE;
        admit(name, GovernanceRequest.virtualTopic(PRINCIPAL, SERVICE, operation, name, request.topic()));
        var value = new VirtualTopicConfig(
                request.topic(),
                request.filter(),
                request.exposePhysicalTopic() != null && request.exposePhysicalTopic(),
                request.valueFormat());
        updater.update(consistency, config -> config.upsertVirtualTopic(name, value));
        return value;
    }

    VirtualTopicConfig updateVirtualTopic(String name, VirtualTopicConfigPatch patch, Consistency consistency) {
        GatewayConfig base = repository.getActiveConfigOrEmpty();
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
        admit(newName, GovernanceRequest.virtualTopic(PRINCIPAL, SERVICE, Operation.ALTER, newName, topic));
        var updated = new VirtualTopicConfig(
                topic,
                patch.filter(),
                patch.exposePhysicalTopic() == null ? current.exposePhysicalTopic() : patch.exposePhysicalTopic(),
                patch.valueFormat());
        updater.update(consistency, config -> replace(config, name, newName, updated));
        return updated;
    }

    void deleteTopic(String name, Consistency consistency) {
        GatewayConfig base = repository.getActiveConfigOrEmpty();
        VirtualTopicConfig virtual = base.virtualTopics().get(name);
        if (virtual != null) {
            admit(name, GovernanceRequest.virtualTopic(PRINCIPAL, SERVICE, Operation.DELETE, name, virtual.topic()));
            updater.update(consistency, config -> config.removeVirtualTopic(name));
            return;
        }
        boolean physicalExists = cache.topics().stream().anyMatch(t -> t.name().equals(name));
        if (!physicalExists) {
            throw new NotFoundException("topic '" + name + "' not found");
        }
        if (governance.hasPhysicalTopicRules(Operation.DELETE)) {
            TopicState current = currentState(name);
            if (current == null) {
                throw new NotFoundException("topic '" + name + "' not found");
            }
            admit(name, GovernanceRequest.topicDelete(PRINCIPAL, SERVICE,
                    new TopicSpec(name, current.partitions(), current.replicationFactor(), current.configs())));
        }
        try {
            topicAdmin.deleteTopic(name);
        } catch (Exception e) {
            throw new IllegalStateException("failed to delete topic '" + name + "': " + e.getMessage(), e);
        }
    }

    TopicSpec createPhysicalTopic(TopicRequest request) {
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
        admit(spec.name(), GovernanceRequest.topic(PRINCIPAL, SERVICE, spec));
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
        List<Violation> violations = governance.evaluate(request);
        if (!violations.isEmpty()) {
            String detail = violations.stream()
                    .map(Violation::describe)
                    .collect(Collectors.joining("; "));
            throw new ForbiddenException("topic '" + name + "' rejected by governance: " + detail);
        }
    }

    int partitionCount(String physicalTopic) {
        return cache.partitionCount(physicalTopic);
    }

    int replicationFactor(String physicalTopic) {
        return cache.replicationFactor(physicalTopic);
    }

    private static GatewayConfig replace(
            GatewayConfig config,
            String currentName,
            String newName,
            VirtualTopicConfig updated
    ) {
        var topics = new HashMap<>(config.virtualTopics());
        topics.remove(currentName);
        topics.put(newName, updated);
        return config.updateVirtualTopics(Map.copyOf(topics));
    }

    private static boolean isTopicExists(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof org.apache.kafka.common.errors.TopicExistsException) {
                return true;
            }
        }
        return false;
    }

    /// Raw virtual and physical topic data returned by [listTopics].
    record TopicListing(List<VirtualTopicEntry> virtual, List<TopicMetadata> physical) {
    }

    /// One virtual topic as known by the [VirtualTopicManager].
    record VirtualTopicEntry(
            String name,
            String physicalTopic,
            VirtualTopicFilterConfig filter,
            boolean exposePhysicalTopic,
            PayloadFormatConfig valueFormat
    ) {
    }
}

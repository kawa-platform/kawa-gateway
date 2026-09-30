package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.GatewayConfigRepository;
import io.jonasg.kawa.config.PayloadFormatConfig;
import io.jonasg.kawa.config.VirtualTopicConfig;
import io.jonasg.kawa.config.VirtualTopicFilterConfig;
import io.jonasg.kawa.core.cluster.MetadataCache;
import io.jonasg.kawa.core.cluster.TopicMetadata;
import io.jonasg.kawa.governance.GovernancePolicy;
import io.jonasg.kawa.governance.TopicSpec;
import io.jonasg.kawa.governance.Violation;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/// Owns the admin topic surface: virtual topic config CRUD, physical topic listing,
/// broker create/delete, and governance admission.
final class TopicService {

    private static final String PRINCIPAL = "admin";
    private static final String SERVICE = "kawa";

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
        if (base.virtualTopics().containsKey(name)) {
            updater.update(consistency, config -> config.removeVirtualTopic(name));
            return;
        }
        boolean physicalExists = cache.topics().stream().anyMatch(t -> t.name().equals(name));
        if (!physicalExists) {
            throw new NotFoundException("topic '" + name + "' not found");
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
        List<Violation> violations = governance.evaluate(PRINCIPAL, SERVICE, spec);
        if (!violations.isEmpty()) {
            String detail = violations.stream()
                    .map(v -> "[" + v.rule() + "] " + v.message())
                    .collect(Collectors.joining("; "));
            throw new ForbiddenException(
                    "topic '" + spec.name() + "' rejected by governance: " + detail);
        }
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

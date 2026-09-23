package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.GatewayConfigRepository;
import io.jonasg.kawa.config.VirtualTopicConfig;
import io.jonasg.kawa.core.cluster.MetadataCache;
import tools.jackson.databind.json.JsonMapper;

import java.util.HashMap;
import java.util.Map;

/// Serves `PATCH /topics/{name}` for partial updates and atomic renames of virtual topics.
public final class PatchTopicHandler implements Router.Handler {

    private final GatewayConfigRepository repository;
    private final MetadataCache cache;
    private final ConsistencyAwareUpdater updater;
    private final JsonMapper mapper = JsonMapper.builder().build();

    public PatchTopicHandler(GatewayConfigRepository repository, MetadataCache cache) {
        this.repository = repository;
        this.cache = cache;
        this.updater = new ConsistencyAwareUpdater(repository);
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        if (!"PATCH".equals(request.method())) {
            return Router.Response.badRequest("unsupported method " + request.method());
        }
        String currentName = request.pathParams().get("name");
        GatewayConfig base = repository.getActiveConfigOrEmpty();
        VirtualTopicConfig current = base.virtualTopics().get(currentName);
        if (current == null) {
            if (cache.topics().stream().anyMatch(topic -> topic.name().equals(currentName))) {
                return Router.Response.badRequest("only virtual topics can be patched");
            }
            return Router.Response.notFound("topic '" + currentName + "' not found");
        }

        VirtualTopicConfigPatch patch;
        try {
            patch = mapper.readValue(request.body(), VirtualTopicConfigPatch.class);
        } catch (Exception e) {
            return Router.Response.badRequest("invalid topic body: " + e.getMessage());
        }

        String newName = patch.name() == null ? currentName : patch.name();
        if (newName.isBlank()) {
            return Router.Response.badRequest("topic name must not be blank");
        }
        if (!newName.equals(currentName) && base.virtualTopics().containsKey(newName)) {
            return Router.Response.conflict("topic '" + newName + "' already exists");
        }

        String topic = patch.topic() == null ? current.topic() : patch.topic();
        if (topic == null || topic.isBlank()) {
            return Router.Response.badRequest("virtual topic requires a physical 'topic'");
        }
        var updated = new VirtualTopicConfig(
                topic,
                patch.filter(),
                patch.exposePhysicalTopic() == null
                        ? current.exposePhysicalTopic()
                        : patch.exposePhysicalTopic(),
                patch.valueFormat());
        try {
            updater.update(request, config -> replace(config, currentName, newName, updated));
        } catch (IllegalArgumentException e) {
            return Router.Response.badRequest(e.getMessage());
        }
        return Router.Response.ok(updated);
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
}

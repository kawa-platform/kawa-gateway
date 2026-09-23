package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfigRepository;
import io.jonasg.kawa.config.VirtualTopicConfig;
import io.jonasg.kawa.governance.GovernancePolicy;
import io.jonasg.kawa.governance.TopicSpec;
import io.jonasg.kawa.governance.Violation;
import org.apache.kafka.common.errors.TopicExistsException;
import tools.jackson.databind.json.JsonMapper;

import java.util.List;
import java.util.stream.Collectors;

/// Serves `POST /topics`: the unified topic creation surface. A `"type": "virtual"` body
/// writes a virtual topic config through the [GatewayConfigRepository]; a
/// `"type": "physical"` body runs the governance admission check and then creates the topic
/// on the broker through the [TopicAdmin].
public final class PostTopicHandler implements Router.Handler {

    /// Placeholder principal until the admin HTTP layer has authentication.
    private static final String PRINCIPAL = "admin";
    /// Placeholder service binding for rules that reference `service`.
    private static final String SERVICE = "kawa";

    private final GovernancePolicy governance;
    private final GatewayConfigRepository repository;
    private final ConsistencyAwareUpdater updater;
    private final TopicAdmin topicAdmin;
    private final JsonMapper mapper = JsonMapper.builder().build();

    public PostTopicHandler(
            GovernancePolicy governance,
            GatewayConfigRepository repository,
            TopicAdmin topicAdmin
    ) {
        this.governance = governance;
        this.repository = repository;
        this.updater = new ConsistencyAwareUpdater(repository);
        this.topicAdmin = topicAdmin;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        if (!"POST".equals(request.method())) {
            return Router.Response.badRequest("unsupported method " + request.method());
        }
        TopicCreateRequest body;
        try {
            body = mapper.readValue(request.body(), TopicCreateRequest.class);
        } catch (Exception e) {
            return Router.Response.badRequest("invalid topic body: " + e.getMessage());
        }
        if (body.name() == null || body.name().isBlank()) {
            return Router.Response.badRequest("topic name is required");
        }
        return switch (body.type()) {
            case "virtual" -> createVirtual(request, body);
            case "physical" -> createPhysical(body);
            default -> Router.Response.badRequest("type must be 'physical' or 'virtual'");
        };
    }

    private Router.Response<?> createVirtual(Router.Request request, TopicCreateRequest body) {
        if (body.topic() == null || body.topic().isBlank()) {
            return Router.Response.badRequest("virtual topic requires a physical 'topic'");
        }
        var config = new VirtualTopicConfig(
                body.topic(),
                body.filter(),
                body.exposePhysicalTopic() != null && body.exposePhysicalTopic(),
                body.valueFormat());
        try {
            updater.update(request, base -> base.upsertVirtualTopic(body.name(), config));
        } catch (IllegalArgumentException e) {
            return Router.Response.badRequest(e.getMessage());
        }
        return Router.Response.created(config);
    }

    private Router.Response<?> createPhysical(TopicCreateRequest body) {
        var spec = new TopicSpec(
                body.name(),
                body.partitions() == null ? -1 : body.partitions(),
                body.replicationFactor() == null ? -1 : body.replicationFactor(),
                body.configs());
        if (governance.exempt(PRINCIPAL, spec.name())) {
            return createOnBroker(spec);
        }
        List<Violation> violations = governance.evaluate(PRINCIPAL, SERVICE, spec);
        if (!violations.isEmpty()) {
            String detail = violations.stream()
                    .map(v -> "[" + v.rule() + "] " + v.message())
                    .collect(Collectors.joining("; "));
            return Router.Response.forbidden(
                    "topic '" + spec.name() + "' rejected by governance: " + detail);
        }
        return createOnBroker(spec);
    }

    private Router.Response<?> createOnBroker(TopicSpec spec) {
        try {
            topicAdmin.createTopic(spec);
            return Router.Response.created(spec);
        } catch (Exception e) {
            if (isTopicExists(e)) {
                return Router.Response.conflict("topic '" + spec.name() + "' already exists");
            }
            return Router.Response.internalError(
                    "failed to create topic '" + spec.name() + "': " + e.getMessage());
        }
    }

    private static boolean isTopicExists(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof TopicExistsException) {
                return true;
            }
        }
        return false;
    }
}

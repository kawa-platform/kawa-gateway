package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfigRepository;
import io.jonasg.kawa.config.VirtualTopicConfig;
import tools.jackson.databind.json.JsonMapper;

/// Serves `PUT /topics/{name}`: upserts the virtual topic config for `name`. The body uses
/// the same `type` discriminator as `POST /topics`; only `"type": "virtual"` is supported
/// (physical topic alteration is not implemented).
public final class PutTopicHandler implements Router.Handler {

    private final GatewayConfigRepository repository;
    private final ConsistencyAwareUpdater updater;
    private final JsonMapper mapper = JsonMapper.builder().build();

    public PutTopicHandler(GatewayConfigRepository repository) {
        this.repository = repository;
        this.updater = new ConsistencyAwareUpdater(repository);
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        if (!"PUT".equals(request.method())) {
            return Router.Response.badRequest("unsupported method " + request.method());
        }
        String name = request.pathParams().get("name");
        TopicCreateRequest body;
        try {
            body = mapper.readValue(request.body(), TopicCreateRequest.class);
        } catch (Exception e) {
            return Router.Response.badRequest("invalid topic body: " + e.getMessage());
        }
        if (!"virtual".equals(body.type())) {
            return Router.Response.badRequest("only 'virtual' topics can be updated");
        }
        if (body.topic() == null || body.topic().isBlank()) {
            return Router.Response.badRequest("virtual topic requires a physical 'topic'");
        }
        var value = new VirtualTopicConfig(
                body.topic(),
                body.filter(),
                body.exposePhysicalTopic() != null && body.exposePhysicalTopic(),
                body.valueFormat());
        try {
            updater.update(request, gatewayCfg -> gatewayCfg.upsertVirtualTopic(name, value));
        } catch (IllegalArgumentException e) {
            return Router.Response.badRequest(e.getMessage());
        }
        return Router.Response.ok(value);
    }
}

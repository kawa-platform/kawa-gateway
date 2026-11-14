package io.jonasg.kawa.http.topics;

import io.jonasg.kawa.http.configchange.ConflictException;
import io.jonasg.kawa.http.configchange.Consistency;
import io.jonasg.kawa.http.configchange.ForbiddenException;
import io.jonasg.kawa.http.web.Router;
import tools.jackson.databind.json.JsonMapper;

import java.util.Map;

/// Serves `POST /topics`: the unified topic creation surface. A physical topic answers `201` once it
/// is in the gateway's metadata, or `202` when it was created but is not visible yet in time.
final class PostTopicHandler implements Router.Handler {

    private final TopicService topicService;
    private final JsonMapper mapper = JsonMapper.builder().build();

    PostTopicHandler(TopicService topicService) {
        this.topicService = topicService;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        TopicRequest body;
        try {
            body = mapper.readValue(request.body(), TopicRequest.class);
        } catch (Exception e) {
            return Router.Response.badRequest("invalid topic body: " + e.getMessage());
        }
        try {
            var consistency = Consistency.fromQueryParam(request.queryParams().get("consistency"));
            return switch (body.type()) {
                case "virtual" -> {
                    var value = topicService.upsertVirtualTopic(request.principal(), body.name(), body, consistency);
                    yield Router.Response.created(new VirtualTopicConfigView(
                            value.topic(),
                            value.filter(),
                            value.exposePhysicalTopic(),
                            value.valueFormat(),
                            value.transformations(),
                            value.managedBy()));
                }
                case "physical" -> {
                    var creation = topicService.createPhysicalTopic(request.principal(), body);
                    var spec = creation.spec();
                    if (!creation.visible()) {
                        yield Router.Response.accepted(Map.of("message", "topic '" + spec.name()
                                + "' was created but is not yet visible in the gateway's metadata"));
                    }
                    yield Router.Response.created(new TopicSpecView(
                            spec.name(),
                            spec.partitions(),
                            spec.replicationFactor(),
                            spec.configs()));
                }
                default -> Router.Response.badRequest("type must be 'physical' or 'virtual'");
            };
        } catch (ConflictException e) {
            return Router.Response.conflict(e.getMessage());
        } catch (ForbiddenException e) {
            return Router.Response.forbidden(e.getMessage());
        } catch (IllegalArgumentException e) {
            return Router.Response.badRequest(e.getMessage());
        } catch (IllegalStateException e) {
            return Router.Response.internalError(e.getMessage());
        }
    }
}

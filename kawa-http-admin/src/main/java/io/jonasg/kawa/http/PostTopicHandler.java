package io.jonasg.kawa.http;

import tools.jackson.databind.json.JsonMapper;

/// Serves `POST /topics`: the unified topic creation surface.
public final class PostTopicHandler implements Router.Handler {

    private final TopicService topicService;
    private final JsonMapper mapper = JsonMapper.builder().build();

    public PostTopicHandler(TopicService topicService) {
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
                    var value = topicService.upsertVirtualTopic(body.name(), body, consistency);
                    yield Router.Response.created(new VirtualTopicConfigView(
                            value.topic(),
                            value.filter(),
                            value.exposePhysicalTopic(),
                            value.valueFormat()));
                }
                case "physical" -> {
                    var spec = topicService.createPhysicalTopic(body);
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

package io.jonasg.kawa.http;

import tools.jackson.databind.json.JsonMapper;

/// Serves `PATCH /topics/{name}` for partial updates and atomic renames of virtual topics.
public final class PatchTopicHandler implements Router.Handler {

    private final TopicService topicService;
    private final JsonMapper mapper = JsonMapper.builder().build();

    public PatchTopicHandler(TopicService topicService) {
        this.topicService = topicService;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        String name = request.pathParams().get("name");
        VirtualTopicConfigPatch patch;
        try {
            patch = mapper.readValue(request.body(), VirtualTopicConfigPatch.class);
        } catch (Exception e) {
            return Router.Response.badRequest("invalid topic body: " + e.getMessage());
        }
        try {
            var consistency = Consistency.fromQueryParam(request.queryParams().get("consistency"));
            var value = topicService.updateVirtualTopic(name, patch, consistency);
            return Router.Response.ok(new VirtualTopicConfigView(
                    value.topic(),
                    value.filter(),
                    value.exposePhysicalTopic(),
                    value.valueFormat()));
        } catch (ConflictException e) {
            return Router.Response.conflict(e.getMessage());
        } catch (NotFoundException e) {
            return Router.Response.notFound(e.getMessage());
        } catch (IllegalArgumentException e) {
            return Router.Response.badRequest(e.getMessage());
        }
    }
}

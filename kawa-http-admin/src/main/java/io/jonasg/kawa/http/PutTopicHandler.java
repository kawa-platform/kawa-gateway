package io.jonasg.kawa.http;

import tools.jackson.databind.json.JsonMapper;

/// Serves `PUT /topics/{name}`: upserts the virtual topic config for `name`.
public final class PutTopicHandler implements Router.Handler {

    private final TopicService topicService;
    private final JsonMapper mapper = JsonMapper.builder().build();

    public PutTopicHandler(TopicService topicService) {
        this.topicService = topicService;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        String name = request.pathParams().get("name");
        TopicRequest body;
        try {
            body = mapper.readValue(request.body(), TopicRequest.class);
        } catch (Exception e) {
            return Router.Response.badRequest("invalid topic body: " + e.getMessage());
        }
        try {
            var consistency = Consistency.fromQueryParam(request.queryParams().get("consistency"));
            var value = topicService.upsertVirtualTopic(name, body, consistency);
            return Router.Response.ok(new VirtualTopicConfigView(
                    value.topic(),
                    value.filter(),
                    value.exposePhysicalTopic(),
                    value.valueFormat()));
        } catch (IllegalArgumentException e) {
            return Router.Response.badRequest(e.getMessage());
        }
    }
}

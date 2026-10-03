package io.jonasg.kawa.http;

/// Serves `DELETE /topics/{name}`: removes a virtual topic's config when `name` is a virtual
/// topic, otherwise deletes the physical topic on the broker through the [TopicAdmin].
/// A name that is both virtual and physical resolves to the virtual config removal.
public final class DeleteTopicHandler implements Router.Handler {

    private final TopicService topicService;

    public DeleteTopicHandler(TopicService topicService) {
        this.topicService = topicService;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        String name = request.pathParams().get("name");
        try {
            var consistency = Consistency.fromQueryParam(request.queryParams().get("consistency"));
            topicService.deleteTopic(name, consistency);
            return Router.Response.noContent();
        } catch (NotFoundException e) {
            return Router.Response.notFound(e.getMessage());
        } catch (ForbiddenException e) {
            return Router.Response.forbidden(e.getMessage());
        } catch (IllegalArgumentException e) {
            return Router.Response.badRequest(e.getMessage());
        } catch (IllegalStateException e) {
            return Router.Response.internalError(e.getMessage());
        }
    }
}

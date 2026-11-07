package io.jonasg.kawa.http.topics;

import io.jonasg.kawa.http.configchange.ConflictException;
import io.jonasg.kawa.http.configchange.Consistency;
import io.jonasg.kawa.http.configchange.ForbiddenException;
import io.jonasg.kawa.http.configchange.NotFoundException;
import io.jonasg.kawa.http.web.Router;

/// Serves `DELETE /topics/{name}`: removes a virtual topic's config when `name` is a virtual
/// topic, otherwise deletes the physical topic on the broker through the [TopicAdmin][io.jonasg.kawa.broker.TopicAdmin].
/// A name that is both virtual and physical resolves to the virtual config removal.
final class DeleteTopicHandler implements Router.Handler {

    private final TopicService topicService;

    DeleteTopicHandler(TopicService topicService) {
        this.topicService = topicService;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        String name = request.pathParams().get("name");
        try {
            var consistency = Consistency.fromQueryParam(request.queryParams().get("consistency"));
            topicService.deleteTopic(request.principal(), name, consistency);
            return Router.Response.noContent();
        } catch (ConflictException e) {
            return Router.Response.conflict(e.getMessage());
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

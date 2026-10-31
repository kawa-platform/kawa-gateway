package io.jonasg.kawa.http.rbac;

import io.jonasg.kawa.http.configchange.ConflictException;
import io.jonasg.kawa.http.configchange.Consistency;
import io.jonasg.kawa.http.configchange.NotFoundException;
import io.jonasg.kawa.http.web.Router;

/// Serves `DELETE /rbac/groups/{name}`.
final class DeleteRbacGroupHandler implements Router.Handler {

    private final RbacGroupService service;

    DeleteRbacGroupHandler(RbacGroupService service) {
        this.service = service;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        String name = request.pathParams().get("name");
        try {
            var consistency = Consistency.fromQueryParam(request.queryParams().get("consistency"));
            service.deleteGroup(request.principal(), name, consistency);
            return Router.Response.noContent();
        } catch (ConflictException e) {
            return Router.Response.conflict(e.getMessage());
        } catch (NotFoundException e) {
            return Router.Response.notFound(e.getMessage());
        } catch (IllegalArgumentException e) {
            return Router.Response.badRequest(e.getMessage());
        }
    }
}

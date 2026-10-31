package io.jonasg.kawa.http.users;

import io.jonasg.kawa.http.configchange.ConflictException;
import io.jonasg.kawa.http.configchange.Consistency;
import io.jonasg.kawa.http.configchange.NotFoundException;
import io.jonasg.kawa.http.web.Router;

/// Serves `DELETE /admin/users/{id}`. Tokens already issued to the user stop working immediately.
/// Deleting your own admin user, or the last enabled one, is refused with a `409`.
final class DeleteAdminUserHandler implements Router.Handler {

    private final AdminUserService service;

    DeleteAdminUserHandler(AdminUserService service) {
        this.service = service;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        String id = request.pathParams().get("id");
        try {
            var consistency = Consistency.fromQueryParam(request.queryParams().get("consistency"));
            service.deleteAdminUser(id, request.principalName(), consistency);
            return Router.Response.noContent();
        } catch (NotFoundException e) {
            return Router.Response.notFound(e.getMessage());
        } catch (ConflictException e) {
            return Router.Response.conflict(e.getMessage());
        } catch (IllegalArgumentException e) {
            return Router.Response.badRequest(e.getMessage());
        }
    }
}

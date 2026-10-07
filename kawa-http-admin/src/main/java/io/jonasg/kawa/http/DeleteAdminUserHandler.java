package io.jonasg.kawa.http;

/// Serves `DELETE /admin/users/{id}`. Tokens already issued to the user stop working immediately.
public final class DeleteAdminUserHandler implements Router.Handler {

    private final AdminUserService service;

    DeleteAdminUserHandler(AdminUserService service) {
        this.service = service;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        String id = request.pathParams().get("id");
        try {
            var consistency = Consistency.fromQueryParam(request.queryParams().get("consistency"));
            service.deleteAdminUser(id, consistency);
            return Router.Response.noContent();
        } catch (NotFoundException e) {
            return Router.Response.notFound(e.getMessage());
        } catch (IllegalArgumentException e) {
            return Router.Response.badRequest(e.getMessage());
        }
    }
}

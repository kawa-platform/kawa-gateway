package io.jonasg.kawa.http;

/// Serves `DELETE /admin/users/{name}`. Tokens already issued to the user stop working immediately.
public final class DeleteAdminUserHandler implements Router.Handler {

    private final AdminUserService service;

    DeleteAdminUserHandler(AdminUserService service) {
        this.service = service;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        String name = request.pathParams().get("name");
        try {
            var consistency = Consistency.fromQueryParam(request.queryParams().get("consistency"));
            service.deleteAdminUser(name, consistency);
            return Router.Response.noContent();
        } catch (NotFoundException e) {
            return Router.Response.notFound(e.getMessage());
        } catch (IllegalArgumentException e) {
            return Router.Response.badRequest(e.getMessage());
        }
    }
}

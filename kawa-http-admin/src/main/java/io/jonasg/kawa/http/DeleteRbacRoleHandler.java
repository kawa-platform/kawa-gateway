package io.jonasg.kawa.http;

/// Serves `DELETE /rbac/roles/{name}`.
public final class DeleteRbacRoleHandler implements Router.Handler {

    private final RbacRoleService service;

    public DeleteRbacRoleHandler(RbacRoleService service) {
        this.service = service;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        String name = request.pathParams().get("name");
        try {
            var consistency = Consistency.fromQueryParam(request.queryParams().get("consistency"));
            service.deleteRole(name, consistency);
            return Router.Response.noContent();
        } catch (NotFoundException e) {
            return Router.Response.notFound(e.getMessage());
        } catch (IllegalArgumentException e) {
            return Router.Response.badRequest(e.getMessage());
        }
    }
}

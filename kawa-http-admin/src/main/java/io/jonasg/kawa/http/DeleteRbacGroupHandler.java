package io.jonasg.kawa.http;

/// Serves `DELETE /rbac/groups/{name}`.
public final class DeleteRbacGroupHandler implements Router.Handler {

    private final RbacGroupService service;

    public DeleteRbacGroupHandler(RbacGroupService service) {
        this.service = service;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        String name = request.pathParams().get("name");
        try {
            var consistency = Consistency.fromQueryParam(request.queryParams().get("consistency"));
            service.deleteGroup(name, consistency);
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

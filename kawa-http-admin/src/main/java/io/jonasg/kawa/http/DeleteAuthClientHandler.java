package io.jonasg.kawa.http;

/// Serves `DELETE /auth/clients/{name}`.
public final class DeleteAuthClientHandler implements Router.Handler {

    private final AuthClientService service;

    public DeleteAuthClientHandler(AuthClientService service) {
        this.service = service;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        String name = request.pathParams().get("name");
        try {
            var consistency = Consistency.fromQueryParam(request.queryParams().get("consistency"));
            service.deleteClient(name, consistency);
            return Router.Response.noContent();
        } catch (NotFoundException e) {
            return Router.Response.notFound(e.getMessage());
        } catch (IllegalArgumentException e) {
            return Router.Response.badRequest(e.getMessage());
        }
    }
}

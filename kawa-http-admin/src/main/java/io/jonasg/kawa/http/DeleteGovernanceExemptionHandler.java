package io.jonasg.kawa.http;

/// Serves `DELETE /governance/exemptions/{name}`.
public final class DeleteGovernanceExemptionHandler implements Router.Handler {

    private final GovernanceService service;

    public DeleteGovernanceExemptionHandler(GovernanceService service) {
        this.service = service;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        var name = request.pathParams().get("name");
        try {
            var consistency = Consistency.fromQueryParam(request.queryParams().get("consistency"));
            service.deleteExemption(name, consistency);
            return Router.Response.noContent();
        } catch (NotFoundException e) {
            return Router.Response.notFound(e.getMessage());
        } catch (IllegalArgumentException e) {
            return Router.Response.badRequest(e.getMessage());
        }
    }
}

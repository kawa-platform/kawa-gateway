package io.jonasg.kawa.http.governance;

import io.jonasg.kawa.http.configchange.Consistency;
import io.jonasg.kawa.http.configchange.NotFoundException;
import io.jonasg.kawa.http.web.Router;

/// Serves `DELETE /governance/variables/{name}`. Refused with `400` while a rule or exemption
/// still reads the variable.
final class DeleteGovernanceVariableHandler implements Router.Handler {

    private final GovernanceService service;

    DeleteGovernanceVariableHandler(GovernanceService service) {
        this.service = service;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        var name = request.pathParams().get("name");
        try {
            var consistency = Consistency.fromQueryParam(request.queryParams().get("consistency"));
            service.deleteVariable(name, consistency);
            return Router.Response.noContent();
        } catch (NotFoundException e) {
            return Router.Response.notFound(e.getMessage());
        } catch (IllegalArgumentException e) {
            return Router.Response.badRequest(e.getMessage());
        }
    }
}

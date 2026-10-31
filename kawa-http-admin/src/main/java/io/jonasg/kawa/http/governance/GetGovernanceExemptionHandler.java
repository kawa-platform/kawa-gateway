package io.jonasg.kawa.http.governance;

import io.jonasg.kawa.http.configchange.NotFoundException;
import io.jonasg.kawa.http.web.Router;

/// Serves `GET /governance/exemptions/{name}`: one global exemption.
final class GetGovernanceExemptionHandler implements Router.Handler {

    private final GovernanceService service;
    private final GovernanceConfigMapper mapper;

    GetGovernanceExemptionHandler(GovernanceService service, GovernanceConfigMapper mapper) {
        this.service = service;
        this.mapper = mapper;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        var name = request.pathParams().get("name");
        try {
            return Router.Response.ok(mapper.toGovernanceExemptionView(service.getExemption(name)));
        } catch (NotFoundException e) {
            return Router.Response.notFound(e.getMessage());
        }
    }
}

package io.jonasg.kawa.http.governance;

import io.jonasg.kawa.http.web.Router;

/// Serves `GET /governance/variables`: every governance variable.
final class GetGovernanceVariablesHandler implements Router.Handler {

    private final GovernanceService service;
    private final GovernanceConfigMapper mapper;

    GetGovernanceVariablesHandler(GovernanceService service, GovernanceConfigMapper mapper) {
        this.service = service;
        this.mapper = mapper;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        return Router.Response.ok(mapper.toGovernanceConfigView(service.get()).variables());
    }
}

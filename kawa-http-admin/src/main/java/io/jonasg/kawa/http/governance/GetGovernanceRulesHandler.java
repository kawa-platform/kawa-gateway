package io.jonasg.kawa.http.governance;

import io.jonasg.kawa.http.web.Router;

/// Serves `GET /governance/rules`: the whole governance section, rules and global exemptions.
final class GetGovernanceRulesHandler implements Router.Handler {

    private final GovernanceService service;
    private final GovernanceConfigMapper mapper;

    GetGovernanceRulesHandler(GovernanceService service, GovernanceConfigMapper mapper) {
        this.service = service;
        this.mapper = mapper;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        return Router.Response.ok(mapper.toGovernanceConfigView(service.get()));
    }
}

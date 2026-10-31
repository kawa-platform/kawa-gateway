package io.jonasg.kawa.http.governance;

import io.jonasg.kawa.http.configchange.NotFoundException;
import io.jonasg.kawa.http.web.Router;

/// Serves `GET /governance/rules/{name}`: one governance rule, including its exemptions.
final class GetGovernanceRuleHandler implements Router.Handler {

    private final GovernanceService service;
    private final GovernanceConfigMapper mapper;

    GetGovernanceRuleHandler(GovernanceService service, GovernanceConfigMapper mapper) {
        this.service = service;
        this.mapper = mapper;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        var name = request.pathParams().get("name");
        try {
            return Router.Response.ok(mapper.toGovernanceRuleConfigView(service.getRule(name)));
        } catch (NotFoundException e) {
            return Router.Response.notFound(e.getMessage());
        }
    }
}

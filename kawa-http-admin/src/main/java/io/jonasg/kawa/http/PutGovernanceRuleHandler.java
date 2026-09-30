package io.jonasg.kawa.http;

import tools.jackson.databind.json.JsonMapper;

/// Serves `PUT /governance/rules/{name}`.
public final class PutGovernanceRuleHandler implements Router.Handler {

    private final GovernanceService service;
    private final GovernanceConfigMapper mapper;
    private final JsonMapper jsonMapper = JsonMapper.builder().build();

    public PutGovernanceRuleHandler(GovernanceService service, GovernanceConfigMapper mapper) {
        this.service = service;
        this.mapper = mapper;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        GovernanceRuleRequest govRuleReq;
        try {
            govRuleReq = jsonMapper.readValue(request.body(), GovernanceRuleRequest.class);
        } catch (Exception e) {
            return Router.Response.badRequest("invalid governance rule body: " + e.getMessage());
        }
        try {
            var consistency = Consistency.fromQueryParam(request.queryParams().get("consistency"));
            var name = request.pathParams().get("name");
            var govRuleCfg = service.upsertRule(mapper.toGovernanceRuleConfig(name, govRuleReq), consistency);
            return Router.Response.ok(mapper.toGovernanceRuleConfigView(govRuleCfg));
        } catch (IllegalArgumentException e) {
            return Router.Response.badRequest(e.getMessage());
        }
    }
}

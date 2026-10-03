package io.jonasg.kawa.http;

import tools.jackson.databind.json.JsonMapper;

/// Serves `PUT /governance/variables/{name}`.
public final class PutGovernanceVariableHandler implements Router.Handler {

    private final GovernanceService service;
    private final GovernanceConfigMapper mapper;
    private final JsonMapper jsonMapper = JsonMapper.builder().build();

    public PutGovernanceVariableHandler(GovernanceService service, GovernanceConfigMapper mapper) {
        this.service = service;
        this.mapper = mapper;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        GovernanceVariableRequest variableReq;
        try {
            variableReq = jsonMapper.readValue(request.body(), GovernanceVariableRequest.class);
        } catch (Exception e) {
            return Router.Response.badRequest("invalid governance variable body: " + e.getMessage());
        }
        try {
            var consistency = Consistency.fromQueryParam(request.queryParams().get("consistency"));
            var name = request.pathParams().get("name");
            var variable = service.upsertVariable(mapper.toGovernanceVariableConfig(name, variableReq), consistency);
            return Router.Response.ok(mapper.toGovernanceVariableConfigView(variable));
        } catch (IllegalArgumentException e) {
            return Router.Response.badRequest(e.getMessage());
        }
    }
}

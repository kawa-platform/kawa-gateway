package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GovernanceConfig;
import tools.jackson.databind.json.JsonMapper;

/// Serves `PUT /governance`.
public final class PutGovernanceHandler implements Router.Handler {

    private final GovernanceService service;
    private final GovernanceConfigMapper mapper = new GovernanceConfigMapper();
    private final JsonMapper jsonMapper = JsonMapper.builder().build();

    public PutGovernanceHandler(GovernanceService service) {
        this.service = service;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        GovernanceConfigRequest body;
        try {
            body = jsonMapper.readValue(request.body(), GovernanceConfigRequest.class);
        } catch (Exception e) {
            return Router.Response.badRequest("invalid governance body: " + e.getMessage());
        }
        try {
            var consistency = Consistency.fromQueryParam(request.queryParams().get("consistency"));
            GovernanceConfig value = service.updateGovernance(body, consistency);
            return Router.Response.ok(mapper.toView(value));
        } catch (IllegalArgumentException e) {
            return Router.Response.badRequest(e.getMessage());
        }
    }
}

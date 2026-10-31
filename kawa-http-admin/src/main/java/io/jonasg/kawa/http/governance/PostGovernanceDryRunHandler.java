package io.jonasg.kawa.http.governance;

import io.jonasg.kawa.governance.GovernanceTrace;
import io.jonasg.kawa.http.web.Router;
import tools.jackson.databind.json.JsonMapper;

/// Serves `POST /governance/dry-run`: evaluates a request against the applied governance, or
/// against one unsaved rule, and returns the full trace. Nothing is created or stored.
final class PostGovernanceDryRunHandler implements Router.Handler {

    private final GovernanceService service;
    private final GovernanceConfigMapper mapper;
    private final JsonMapper jsonMapper = JsonMapper.builder().build();

    PostGovernanceDryRunHandler(GovernanceService service, GovernanceConfigMapper mapper) {
        this.service = service;
        this.mapper = mapper;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        GovernanceDryRunRequest dryRunReq;
        try {
            dryRunReq = jsonMapper.readValue(request.body(), GovernanceDryRunRequest.class);
        } catch (Exception e) {
            return Router.Response.badRequest("invalid governance dry-run body: " + e.getMessage());
        }
        try {
            var governanceRequest = mapper.toGovernanceRequest(dryRunReq);
            GovernanceTrace trace;
            if (dryRunReq.rule() == null) {
                trace = service.dryRun(governanceRequest);
            } else {
                var name = dryRunReq.rule().name() == null || dryRunReq.rule().name().isBlank() ? "draft" : dryRunReq.rule().name();
                var rule = mapper.toGovernanceRuleConfig(name, dryRunReq.rule(), service.get().variables().values());
                trace = service.dryRun(governanceRequest, rule);
            }
            return Router.Response.ok(mapper.toGovernanceDryRunView(trace));
        } catch (IllegalArgumentException e) {
            return Router.Response.badRequest(e.getMessage());
        }
    }
}

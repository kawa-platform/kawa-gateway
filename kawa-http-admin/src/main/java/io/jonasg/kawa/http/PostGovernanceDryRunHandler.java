package io.jonasg.kawa.http;

import io.jonasg.kawa.governance.GovernancePolicy;
import io.jonasg.kawa.governance.GovernanceTrace;
import tools.jackson.databind.json.JsonMapper;

/// Serves `POST /governance/dry-run`: evaluates a request against the applied governance, or
/// against one unsaved rule, and returns the full trace. Nothing is created or stored.
public final class PostGovernanceDryRunHandler implements Router.Handler {

    private final GovernancePolicy policy;
    private final GovernanceService service;
    private final GovernanceConfigMapper mapper;
    private final JsonMapper jsonMapper = JsonMapper.builder().build();

    public PostGovernanceDryRunHandler(GovernancePolicy policy, GovernanceService service, GovernanceConfigMapper mapper) {
        this.policy = policy;
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
                trace = policy.dryRun(governanceRequest);
            } else {
                var name = dryRunReq.rule().name() == null || dryRunReq.rule().name().isBlank() ? "draft" : dryRunReq.rule().name();
                var rule = mapper.toGovernanceRuleConfig(name, dryRunReq.rule(), service.get().variables().values());
                trace = policy.dryRun(governanceRequest, rule);
            }
            return Router.Response.ok(mapper.toGovernanceDryRunView(trace));
        } catch (IllegalArgumentException e) {
            return Router.Response.badRequest(e.getMessage());
        }
    }
}

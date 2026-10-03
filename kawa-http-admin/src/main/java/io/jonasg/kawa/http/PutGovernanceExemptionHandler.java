package io.jonasg.kawa.http;

import tools.jackson.databind.json.JsonMapper;

/// Serves `PUT /governance/exemptions/{name}`.
public final class PutGovernanceExemptionHandler implements Router.Handler {

    private final GovernanceService service;
    private final GovernanceConfigMapper mapper;
    private final JsonMapper jsonMapper = JsonMapper.builder().build();

    public PutGovernanceExemptionHandler(GovernanceService service, GovernanceConfigMapper mapper) {
        this.service = service;
        this.mapper = mapper;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        GovernanceExemptionRequest exemptionReq;
        try {
            exemptionReq = jsonMapper.readValue(request.body(), GovernanceExemptionRequest.class);
        } catch (Exception e) {
            return Router.Response.badRequest("invalid governance exemption body: " + e.getMessage());
        }
        try {
            var consistency = Consistency.fromQueryParam(request.queryParams().get("consistency"));
            var name = request.pathParams().get("name");
            var exemption = service.upsertExemption(mapper.toGovernanceExemption(name, exemptionReq, service.get().variables().values()), consistency);
            return Router.Response.ok(mapper.toGovernanceExemptionView(exemption));
        } catch (IllegalArgumentException e) {
            return Router.Response.badRequest(e.getMessage());
        }
    }
}

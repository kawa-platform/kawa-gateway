package io.jonasg.kawa.http;

/// Serves `GET /governance/exemptions`: every global exemption.
public final class GetGovernanceExemptionsHandler implements Router.Handler {

    private final GovernanceService service;
    private final GovernanceConfigMapper mapper;

    public GetGovernanceExemptionsHandler(GovernanceService service, GovernanceConfigMapper mapper) {
        this.service = service;
        this.mapper = mapper;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        return Router.Response.ok(mapper.toGovernanceConfigView(service.get()).exemptions());
    }
}

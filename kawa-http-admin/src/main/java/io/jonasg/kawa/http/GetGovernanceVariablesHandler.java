package io.jonasg.kawa.http;

/// Serves `GET /governance/variables`: every governance variable.
public final class GetGovernanceVariablesHandler implements Router.Handler {

    private final GovernanceService service;
    private final GovernanceConfigMapper mapper;

    public GetGovernanceVariablesHandler(GovernanceService service, GovernanceConfigMapper mapper) {
        this.service = service;
        this.mapper = mapper;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        return Router.Response.ok(mapper.toGovernanceConfigView(service.get()).variables());
    }
}

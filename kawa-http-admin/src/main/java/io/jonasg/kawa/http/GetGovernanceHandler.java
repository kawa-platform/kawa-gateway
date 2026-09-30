package io.jonasg.kawa.http;

/// Serves `GET /governance/rules`.
public final class GetGovernanceHandler implements Router.Handler {

    private final GovernanceService service;
    private final GovernanceConfigMapper mapper;

    public GetGovernanceHandler(GovernanceService service, GovernanceConfigMapper mapper) {
        this.service = service;
        this.mapper = mapper;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        return Router.Response.ok(mapper.toGovernanceRuleConfigView(service.get()));
    }
}

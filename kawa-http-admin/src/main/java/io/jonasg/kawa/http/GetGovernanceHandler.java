package io.jonasg.kawa.http;

/// Serves `GET /governance`.
public final class GetGovernanceHandler implements Router.Handler {

    private final GovernanceService service;
    private final GovernanceConfigMapper mapper = new GovernanceConfigMapper();

    public GetGovernanceHandler(GovernanceService service) {
        this.service = service;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        return Router.Response.ok(mapper.toView(service.get()));
    }
}

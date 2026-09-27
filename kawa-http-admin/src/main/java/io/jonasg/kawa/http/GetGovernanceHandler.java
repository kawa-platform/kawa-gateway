package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfigRepository;

/// Serves `GET /governance`.
public final class GetGovernanceHandler implements Router.Handler {

    private final GatewayConfigRepository repository;
    private final GovernanceConfigMapper mapper = new GovernanceConfigMapper();

    public GetGovernanceHandler(GatewayConfigRepository repository) {
        this.repository = repository;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        return Router.Response.ok(mapper.toView(repository.getActiveConfigOrEmpty().governance()));
    }
}

package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfigRepository;
import io.jonasg.kawa.config.GovernanceConfig;
import tools.jackson.databind.json.JsonMapper;

/// Serves `PUT /governance`.
public final class PutGovernanceHandler implements Router.Handler {

    private final ConsistencyAwareUpdater updater;
    private final JsonMapper mapper = JsonMapper.builder().build();
    private final GovernanceConfigMapper governanceMapper = new GovernanceConfigMapper();

    public PutGovernanceHandler(GatewayConfigRepository repository) {
        this.updater = new ConsistencyAwareUpdater(repository);
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        GovernanceConfigRequest body;
        try {
            body = mapper.readValue(request.body(), GovernanceConfigRequest.class);
        } catch (Exception e) {
            return Router.Response.badRequest("invalid governance body: " + e.getMessage());
        }
        GovernanceConfig value;
        try {
            value = governanceMapper.toConfig(body);
        } catch (IllegalArgumentException e) {
            return Router.Response.badRequest(e.getMessage());
        }
        try {
            updater.update(request, config -> config.updateGovernance(value));
        } catch (IllegalArgumentException e) {
            return Router.Response.badRequest(e.getMessage());
        }
        return Router.Response.ok(governanceMapper.toView(value));
    }
}

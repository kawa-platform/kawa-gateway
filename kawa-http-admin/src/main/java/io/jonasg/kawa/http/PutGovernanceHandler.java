package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfigRepository;
import io.jonasg.kawa.config.GovernanceConfig;
import io.jonasg.kawa.governance.GovernancePolicy;
import tools.jackson.databind.json.JsonMapper;

/// Serves `PUT /governance`.
public final class PutGovernanceHandler implements Router.Handler {

    private final GatewayConfigRepository repository;
    private final ConsistencyAwareUpdater updater;
    private final GovernancePolicy governance;
    private final JsonMapper mapper = JsonMapper.builder().build();
    private final GovernanceConfigMapper governanceMapper = new GovernanceConfigMapper();

    public PutGovernanceHandler(GatewayConfigRepository repository, GovernancePolicy governance) {
        this.repository = repository;
        this.updater = new ConsistencyAwareUpdater(repository);
        this.governance = governance;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        GovernanceConfigRequest body;
        try {
            body = mapper.readValue(request.body(), GovernanceConfigRequest.class);
        } catch (Exception e) {
            return Router.Response.badRequest("invalid governance body: " + e.getMessage());
        }
        // the mapper validates, so its rejections must not be folded into the deserialization
        // message above: each one already names the rule or exemption at fault
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

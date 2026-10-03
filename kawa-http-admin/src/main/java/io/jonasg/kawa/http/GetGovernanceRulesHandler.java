package io.jonasg.kawa.http;

/// Serves `GET /governance/rules`: the whole governance section, rules and global exemptions.
public final class GetGovernanceRulesHandler implements Router.Handler {

    private final GovernanceService service;
    private final GovernanceConfigMapper mapper;

    public GetGovernanceRulesHandler(GovernanceService service, GovernanceConfigMapper mapper) {
        this.service = service;
        this.mapper = mapper;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        return Router.Response.ok(mapper.toGovernanceConfigView(service.get()));
    }
}

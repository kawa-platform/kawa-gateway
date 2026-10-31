package io.jonasg.kawa.http.governance;

import io.jonasg.kawa.http.web.Router;

/// Registers the `/governance` routes, backed by the [GovernanceService], on a [Router].
public final class GovernanceRoutes {

    private GovernanceRoutes() {
    }

    public static void register(Router router, GovernanceService governanceService) {
        var mapper = new GovernanceConfigMapper();
        router.get("/governance/rules", new GetGovernanceRulesHandler(governanceService, mapper))
                .get("/governance/rules/{name}", new GetGovernanceRuleHandler(governanceService, mapper))
                .put("/governance/rules/{name}", new PutGovernanceRuleHandler(governanceService, mapper))
                .delete("/governance/rules/{name}", new DeleteGovernanceRuleHandler(governanceService))
                .get("/governance/exemptions", new GetGovernanceExemptionsHandler(governanceService, mapper))
                .get("/governance/exemptions/{name}", new GetGovernanceExemptionHandler(governanceService, mapper))
                .put("/governance/exemptions/{name}", new PutGovernanceExemptionHandler(governanceService, mapper))
                .delete("/governance/exemptions/{name}", new DeleteGovernanceExemptionHandler(governanceService))
                .post("/governance/dry-run", new PostGovernanceDryRunHandler(governanceService, mapper))
                .get("/governance/variables", new GetGovernanceVariablesHandler(governanceService, mapper))
                .put("/governance/variables/{name}", new PutGovernanceVariableHandler(governanceService, mapper))
                .delete("/governance/variables/{name}", new DeleteGovernanceVariableHandler(governanceService));
    }
}

package io.jonasg.kawa.http.users;

import io.jonasg.kawa.http.web.Router;

/// Serves the public `GET /admin/setup`: whether kawa studio has to offer creating the first admin user.
final class GetSetupHandler implements Router.Handler {

    private final AdminSetup setup;

    GetSetupHandler(AdminSetup setup) {
        this.setup = setup;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        return Router.Response.ok(new SetupView(setup.required()));
    }
}

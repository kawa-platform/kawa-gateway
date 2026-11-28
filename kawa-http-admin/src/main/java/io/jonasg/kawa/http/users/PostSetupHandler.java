package io.jonasg.kawa.http.users;

import io.jonasg.kawa.http.configchange.ConflictException;
import io.jonasg.kawa.http.web.Router;

import java.util.Map;

/// Serves the public `POST /admin/setup`: creates the first admin user of a gateway in setup mode and
/// answers `201` with it, like `POST /admin/users`. `409` when setup is not required, `401` for a missing
/// or wrong setup token, `400` for an invalid body. The token is checked before the admin user, so only
/// a caller holding it learns why an admin user is refused.
final class PostSetupHandler implements Router.Handler {

    private final AdminSetup setup;
    private final AdminUserMapper mapper;

    PostSetupHandler(AdminSetup setup, AdminUserMapper mapper) {
        this.setup = setup;
        this.mapper = mapper;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        if (!setup.required()) {
            return Router.Response.conflict(AdminSetup.NOT_REQUIRED);
        }
        try {
            var body = read(request.body());
            if (!setup.acceptsToken(body.token())) {
                return invalidToken();
            }
            var user = mapper.toNewAdminUser(new CreateAdminUserRequest(body.email(), body.password(), null));
            String id = setup.createFirstAdminUser(body.token(), user);
            return Router.Response.created(mapper.toAdminUserView(id, user)).withHeader("Location", "/admin/users/" + id);
        } catch (AdminSetup.InvalidSetupTokenException e) {
            return invalidToken();
        } catch (ConflictException e) {
            return Router.Response.conflict(e.getMessage());
        } catch (IllegalArgumentException e) {
            return Router.Response.badRequest(e.getMessage());
        }
    }

    private static SetupRequest read(byte[] body) {
        try {
            return AdminUserRequests.read(body, SetupRequest.class);
        } catch (IllegalArgumentException e) {
            Throwable parseError = e.getCause() == null ? e : e.getCause();
            throw new IllegalArgumentException("invalid setup body: " + parseError.getMessage(), e);
        }
    }

    private static Router.Response<Map<String, String>> invalidToken() {
        return new Router.Response<>(401, Map.of("error", "invalid setup token"), "application/json", false);
    }
}

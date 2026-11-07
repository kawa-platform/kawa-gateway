package io.jonasg.kawa.http.oauthclients;

import io.jonasg.kawa.http.configchange.ConflictException;
import io.jonasg.kawa.http.configchange.Consistency;
import io.jonasg.kawa.http.configchange.NotFoundException;
import io.jonasg.kawa.http.web.Router;

/// Serves `DELETE /admin/oauth-clients/{id}`. An unknown id is a `404`; a reserved id (`kawa-studio`, `kawa-operator`) is a `409`.
final class DeleteOAuthClientHandler implements Router.Handler {

    private final OAuthClientService service;

    DeleteOAuthClientHandler(OAuthClientService service) {
        this.service = service;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        try {
            var consistency = Consistency.fromQueryParam(request.queryParams().get("consistency"));
            service.deleteOAuthClient(request.pathParams().get("id"), consistency);
            return Router.Response.noContent();
        } catch (NotFoundException e) {
            return Router.Response.notFound(e.getMessage());
        } catch (ConflictException e) {
            return Router.Response.conflict(e.getMessage());
        } catch (IllegalArgumentException e) {
            return Router.Response.badRequest(e.getMessage());
        }
    }
}

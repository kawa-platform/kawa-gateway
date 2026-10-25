package io.jonasg.kawa.http;

import io.jonasg.kawa.plugin.auth.KawaPrincipal;
import org.jspecify.annotations.Nullable;

/// Serves `DELETE /{section}/{name}/managed-by` for every managed section (virtual topics, roles,
/// groups, clients): releases the entry from its owner so it becomes editable by anyone again.
/// `204` when released or already unmanaged, `404` when the entry is missing, `409` when the
/// caller is not the owner.
public final class DeleteManagedByHandler implements Router.Handler {

    /// A section's release operation, e.g. [RbacRoleService#releaseRole].
    @FunctionalInterface
    interface Release {
        void release(@Nullable KawaPrincipal caller, String name, Consistency consistency);
    }

    private final Release release;

    DeleteManagedByHandler(Release release) {
        this.release = release;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        String name = request.pathParams().get("name");
        try {
            var consistency = Consistency.fromQueryParam(request.queryParams().get("consistency"));
            release.release(request.principal(), name, consistency);
            return Router.Response.noContent();
        } catch (ConflictException e) {
            return Router.Response.conflict(e.getMessage());
        } catch (NotFoundException e) {
            return Router.Response.notFound(e.getMessage());
        } catch (IllegalArgumentException e) {
            return Router.Response.badRequest(e.getMessage());
        }
    }
}

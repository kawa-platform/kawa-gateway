package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.GatewayConfigRepository;
import org.jspecify.annotations.Nullable;
import tools.jackson.databind.json.JsonMapper;

import java.util.List;
import java.util.Map;

/// Base for the per-section config handlers (`/rbac/...`, `/auth/...`). Each subclass maps one dynamic
/// config section (virtual topics, RBAC roles/groups, auth users) onto the [GatewayConfig]
/// snapshot: [entries] reads the section, [upsert] and [remove] produce a new snapshot with
/// one entry changed. `C` is the config record the section stores and `R` the transport body the
/// endpoint accepts, so the config model and the wire model are free to differ; `G` and `P` are
/// the `GET` list element and the `PUT` response body, named after the method that produces each,
/// which keeps [listView] and [putView] from handing back an untyped value. A section whose list
/// item and `PUT` body coincide names the same type twice. Plain handler with no Netty imports;
/// the [HttpRouterHandler] dispatcher serializes the result and writes the response.
///
/// `GET` lists the section, `PUT /{name}` upserts one entry (persisting the new snapshot via
/// the [GatewayConfigRepository] and returning the stored entry), `DELETE /{name}` removes it
/// (404 when it does not exist). The section starts empty when no snapshot has been applied yet.
abstract class BaseCRUDHandler<C, R, G, P> {

    protected final GatewayConfigRepository repository;
    protected final ConsistencyAwareUpdater updater;
    protected final JsonMapper mapper = JsonMapper.builder().build();
    private final Class<R> requestType;
    private final String sectionName;

    BaseCRUDHandler(GatewayConfigRepository repository, Class<R> requestType, String sectionName) {
        this.repository = repository;
        this.updater = new ConsistencyAwareUpdater(repository);
        this.requestType = requestType;
        this.sectionName = sectionName;
    }

    /// The request body mapped onto a config entry. Null-defaulting must match the config
    /// record exactly, so the accept/reject boundary does not move.
    protected abstract C toConfig(String name, R body);

    /// The `PUT` response body: the stored entry as written, in the shape `P` names. The name is
    /// on the path the caller just wrote, so a section may omit it from the body, which is why `P`
    /// is often a `*PutView` rather than the same type as `G`.
    protected abstract P putView(String name, C value);

    /// The section's entries from a snapshot.
    protected abstract Map<String, C> entries(GatewayConfig config);

    /// The `GET` response body: one `G` per entry, each carrying the entry name the map key holds,
    /// since a list has no path to read it from. Abstract rather than defaulted to the raw entries
    /// map, because a response body is a transport type and never a `kawa-config` record.
    protected abstract List<G> listView(GatewayConfig config);

    /// A new snapshot with the given entry added or replaced.
    protected abstract GatewayConfig upsert(GatewayConfig config, String name, C value);

    /// A new snapshot with the given entry removed.
    protected abstract GatewayConfig remove(GatewayConfig config, String name);

    /// Guards the removal of an entry. Base implementation always allows the removal;
    /// subclasses override for referential-integrity rules (e.g. "cannot delete a group
    /// that still lists clients") when a section exposes a `DELETE`.
    ///
    /// @return a non-`null` response to short-circuit the removal, or `null` to allow it
    protected Router.@Nullable Response<?> validateRemove(GatewayConfig config, String name) {
        return null;
    }

    @SuppressWarnings("unused")
    Router.Response<?> get(Router.Request request) {
        GatewayConfig base = repository.getActiveConfigOrEmpty();
        return Router.Response.ok(listView(base));
    }

    Router.Response<?> put(Router.Request request) {
        String name = request.pathParams().get("name");
        R body;
        try {
            body = mapper.readValue(request.body(), requestType);
        } catch (Exception e) {
            return Router.Response.badRequest("invalid " + sectionName + " body: " + e.getMessage());
        }
        C value;
        try {
            value = toConfig(name, body);
        } catch (IllegalArgumentException e) {
            return Router.Response.badRequest(e.getMessage());
        }
        try {
            updater.update(request, config -> upsert(config, name, value));
        } catch (IllegalArgumentException e) {
            return Router.Response.badRequest(e.getMessage());
        }
        return Router.Response.ok(putView(name, value));
    }

    Router.Response<?> delete(Router.Request request) {
        GatewayConfig base = repository.getActiveConfigOrEmpty();
        String name = request.pathParams().get("name");
        if (!entries(base).containsKey(name)) {
            return Router.Response.notFound(sectionName + " '" + name + "' not found");
        }
        Router.Response<?> rejection = validateRemove(base, name);
        if (rejection != null) {
            return rejection;
        }
        try {
            updater.update(request, config -> remove(config, name));
        } catch (IllegalArgumentException e) {
            return Router.Response.badRequest(e.getMessage());
        }
        return Router.Response.noContent();
    }
}

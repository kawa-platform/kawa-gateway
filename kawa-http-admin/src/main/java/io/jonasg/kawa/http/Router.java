package io.jonasg.kawa.http;

import io.jonasg.kawa.plugin.auth.KawaPrincipal;
import io.netty.handler.codec.http.HttpMethod;
import org.jspecify.annotations.NullUnmarked;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/// A tiny, data-driven HTTP route table. Routes are keyed by `(method, path template)` so that a
/// path registered for one method but hit with another can be distinguished from a wholly unknown
/// path. A template may contain `{name}` segments that match any single path segment and are
/// captured into [Request#pathParams]. Routes require an authenticated caller unless registered
/// as public (see [#publicGet] and [#publicRoute]).
public final class Router {

    /// A route handler. Implementations are plain (no Netty imports); the [HttpRouterHandler]
    /// dispatcher is responsible for serializing the returned [Response] body and writing the HTTP
    /// response.
    @FunctionalInterface
    public interface Handler {
        Response<?> handle(Request request);
    }

    /// The decoded request handed to a handler: method, path, path parameters captured from
    /// `{name}` template segments, query parameters, the request headers (keyed by lower-case
    /// name), the raw request body (empty when the request has none), and the authenticated
    /// caller (`null` on public routes).
    public record Request(
            String method,
            String path,
            Map<String, String> pathParams,
            Map<String, String> queryParams,
            Map<String, List<String>> headers,
            byte[] body,
            @Nullable KawaPrincipal principal
    ) {

        /// The first value of the header `name` (lower-case), if present.
        public Optional<String> header(String name) {
            List<String> values = headers.get(name);
            return values == null || values.isEmpty() ? Optional.empty() : Optional.of(values.getFirst());
        }

        /// The name of the authenticated caller, or `null` on a public route. Under kawa's `oauth`
        /// provider that is the admin user's current email.
        public @Nullable String principalName() {
            return principal == null ? null : principal.name();
        }
    }

    /// The response a handler returns: an HTTP status plus a body object serialized to JSON by the
    /// dispatcher. A `null` body serializes to an empty body. A raw response carries a
    /// pre-serialized body (e.g. YAML) written verbatim with [Response#contentType]. `headers` are
    /// extra response headers.
    @NullUnmarked
    public record Response<T>(int status, T body, String contentType, boolean rawBody, Map<String, String> headers) {

        private static final String JSON = "application/json";

        public Response(int status, T body, String contentType, boolean rawBody) {
            this(status, body, contentType, rawBody, Map.of());
        }

        public static <T> Response<T> ok(T body) {
            return new Response<>(200, body, JSON, false);
        }

        public static <T> Response<T> created(T body) {
            return new Response<>(201, body, JSON, false);
        }

        public static Response<Void> noContent() {
            return new Response<>(204, null, JSON, false);
        }

        /// `401 Unauthorized` with an RFC 6750 bearer challenge.
        public static Response<Map<String, String>> unauthorized() {
            return new Response<>(401, Map.of("error", "unauthorized"), JSON, false,
                    Map.of("WWW-Authenticate", "Bearer realm=\"kawa\""));
        }

        /// A copy of this response with the header `name` set to `value`.
        public Response<T> withHeader(String name, String value) {
            var merged = new HashMap<>(headers);
            merged.put(name, value);
            return new Response<>(status, body, contentType, rawBody, Map.copyOf(merged));
        }

        public static Response<Map<String, String>> badRequest(String message) {
            return error(400, message);
        }

        public static Response<Map<String, String>> notFound(String message) {
            return error(404, message);
        }

        public static Response<Map<String, String>> conflict(String message) {
            return error(409, message);
        }

        public static Response<Map<String, String>> forbidden(String message) {
            return error(403, message);
        }

        public static Response<Map<String, String>> internalError(String message) {
            return error(500, message);
        }

        /// A raw response: `body` is written verbatim with `contentType` instead of being
        /// JSON-serialized by the dispatcher.
        public static Response<String> raw(int status, String body, String contentType) {
            return new Response<>(status, body, contentType, true);
        }

        private static Response<Map<String, String>> error(int status, String message) {
            return new Response<>(status, Map.of("error", message), JSON, false);
        }
    }

    /// A matched route: the handler, the path parameters captured from `{name}` segments, and
    /// whether the route is reachable without authentication.
    public record Match(Handler handler, Map<String, String> pathParams, boolean isPublic) {
    }

    private record Route(
            HttpMethod method,
            List<String> segments,
            Handler handler,
            boolean isPublic
    ) {
        public boolean matches(HttpMethod method, List<String> segments) {
            return this.method() == method && matchesTemplate(segments(), segments);
        }
    }

    private final List<Route> routes = new ArrayList<>();

    /// Registers a handler for `GET path` and returns `this` for chaining.
    public Router get(String path, Handler handler) {
        return route(HttpMethod.GET, path, handler);
    }

    /// Registers a handler for `PUT path` and returns `this` for chaining.
    public Router put(String path, Handler handler) {
        return route(HttpMethod.PUT, path, handler);
    }

    /// Registers a handler for `POST path` and returns `this` for chaining.
    public Router post(String path, Handler handler) {
        return route(HttpMethod.POST, path, handler);
    }

    /// Registers a handler for `PATCH path` and returns `this` for chaining.
    public Router patch(String path, Handler handler) {
        return route(HttpMethod.PATCH, path, handler);
    }

    /// Registers a handler for `DELETE path` and returns `this` for chaining.
    public Router delete(String path, Handler handler) {
        return route(HttpMethod.DELETE, path, handler);
    }

    /// Registers a handler for `GET path` that needs no authentication.
    public Router publicGet(String path, Handler handler) {
        return publicRoute(HttpMethod.GET, path, handler);
    }

    /// Registers a handler for `method path` that needs no authentication.
    public Router publicRoute(HttpMethod method, String path, Handler handler) {
        return route(method, path, handler, true);
    }

    private Router route(HttpMethod method, String path, Handler handler) {
        return route(method, path, handler, false);
    }

    private Router route(HttpMethod method, String path, Handler handler, boolean isPublic) {
        routes.add(new Route(method, split(path), handler, isPublic));
        return this;
    }

    /// Returns the handler registered for `(method, path)`, if any, along with the path parameters
    /// captured from `{name}` template segments.
    public Optional<Match> find(HttpMethod method, String path) {
        List<String> segments = split(path);
        return this.routes.stream()
                .filter(route -> route.matches(method, segments))
                .map(route -> new Match(route.handler(), getParams(route, segments), route.isPublic()))
                .findFirst();
    }

    private static Map<String, String> getParams(Route route, List<String> segments) {
        Map<String, String> params = new HashMap<>();
        for (int i = 0; i < segments.size(); i++) {
            String template = route.segments().get(i);
            if (template.startsWith("{") && template.endsWith("}")) {
                params.put(template.substring(1, template.length() - 1), segments.get(i));
            }
        }
        return params;
    }

    /// Returns whether any route is registered for `path` regardless of method.
    public boolean hasPath(String path) {
        List<String> segments = split(path);
        return routes.stream().anyMatch(route -> matchesTemplate(route.segments(), segments));
    }

    private static boolean matchesTemplate(List<String> template, List<String> actual) {
        if (template.size() != actual.size()) {
            return false;
        }
        for (int i = 0; i < template.size(); i++) {
            String t = template.get(i);
            if (!t.startsWith("{") && !t.equals(actual.get(i))) {
                return false;
            }
        }
        return true;
    }

    private static List<String> split(String path) {
        String trimmed = path.startsWith("/") ? path.substring(1) : path;
        if (trimmed.isEmpty()) {
            return List.of();
        }
        return List.of(trimmed.split("/"));
    }
}

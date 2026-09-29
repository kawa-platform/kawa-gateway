package io.jonasg.kawa.http;

import io.netty.handler.codec.http.HttpMethod;
import org.jspecify.annotations.NullUnmarked;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/// A tiny, data-driven HTTP route table. Routes are keyed by `(method, path template)` so that a
/// path registered for one method but hit with another can be distinguished from a wholly unknown
/// path. A template may contain `{name}` segments that match any single path segment and are
/// captured into [Request#pathParams].
public final class Router {

    /// A route handler. Implementations are plain (no Netty imports); the [HttpRouterHandler]
    /// dispatcher is responsible for serializing the returned [Response] body and writing the HTTP
    /// response.
    @FunctionalInterface
    public interface Handler {
        Response<?> handle(Request request);
    }

    /// The decoded request handed to a handler: method, path, path parameters captured from
    /// `{name}` template segments, query parameters, and the raw request body (empty when the
    /// request has none).
    public record Request(
            String method,
            String path,
            Map<String, String> pathParams,
            Map<String, String> queryParams,
            byte[] body
    ) { }

    /// The response a handler returns: an HTTP status plus a body object serialized to JSON by the
    /// dispatcher. A `null` body serializes to an empty body. A raw response carries a
    /// pre-serialized body (e.g. YAML) written verbatim with [Response#contentType].
    @NullUnmarked
    public record Response<T>(int status, T body, String contentType, boolean rawBody) {

        private static final String JSON = "application/json";

        public static <T> Response<T> ok(T body) {
            return new Response<>(200, body, JSON, false);
        }

        public static <T> Response<T> created(T body) {
            return new Response<>(201, body, JSON, false);
        }

        public static Response<Void> noContent() {
            return new Response<>(204, null, JSON, false);
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

    /// A matched route: the handler plus the path parameters captured from `{name}` segments.
    public record Match(Handler handler, Map<String, String> pathParams) {
    }

    private record Route(HttpMethod method, List<String> segments, Handler handler) {
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

    private Router route(HttpMethod method, String path, Handler handler) {
        routes.add(new Route(method, split(path), handler));
        return this;
    }

    /// Returns the handler registered for `(method, path)`, if any, along with the path parameters
    /// captured from `{name}` template segments.
    public Optional<Match> find(HttpMethod method, String path) {
        List<String> segments = split(path);
        for (Route route : routes) {
            if (route.method() != method || !matches(route.segments(), segments)) {
                continue;
            }
            Map<String, String> params = new HashMap<>();
            for (int i = 0; i < segments.size(); i++) {
                String template = route.segments().get(i);
                if (template.startsWith("{") && template.endsWith("}")) {
                    params.put(template.substring(1, template.length() - 1), segments.get(i));
                }
            }
            return Optional.of(new Match(route.handler(), params));
        }
        return Optional.empty();
    }

    /// Returns whether any route is registered for `path` regardless of method.
    public boolean hasPath(String path) {
        List<String> segments = split(path);
        return routes.stream().anyMatch(route -> matches(route.segments(), segments));
    }

    private static boolean matches(List<String> template, List<String> actual) {
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

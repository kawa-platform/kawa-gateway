package io.jonasg.kawa.mcp;

import io.jonasg.kawa.config.CorsConfig;
import io.jonasg.kawa.http.Router;
import io.jonasg.kawa.plugin.auth.KawaPrincipal;
import io.netty.handler.codec.http.HttpMethod;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static org.assertj.core.api.Assertions.assertThat;

class McpEndpointTest {

    private static final KawaPrincipal ALICE = new KawaPrincipal("alice");

    private final AtomicReference<@Nullable KawaPrincipal> topicsCaller = new AtomicReference<>();
    private Router router;

    @BeforeEach
    void setUp() {
        router = new Router().get("/topics", request -> {
            topicsCaller.set(request.principal());
            return Router.Response.ok(List.of(Map.of("type", "physical", "name", "orders")));
        });
    }

    @Test
    void initializeAdvertisesServerInfoAndTools() {
        // given
        new KawaMcpServer(router, null);

        // when
        var response = post(rpc(1, "initialize", """
                {"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"test","version":"1"}}"""));

        // then
        assertThat(response.status())
                .withFailMessage(() -> "initialize was not answered with 200 but " + response.status())
                .isEqualTo(200);
        assertThatJson(body(response)).inPath("result.serverInfo.name").isEqualTo("kawa");
        assertThatJson(body(response)).inPath("result.capabilities.tools").isPresent();
    }

    @Test
    void listsTheListTopicsTool() {
        // given
        new KawaMcpServer(router, null);

        // when
        var response = post(rpc(2, "tools/list", "{}"));

        // then
        assertThatJson(body(response)).inPath("result.tools[*].name").isArray().containsExactly("list_topics");
    }

    @Test
    void listTopicsDispatchesToTheTopicsRouteAsTheCaller() {
        // given
        new KawaMcpServer(router, null);

        // when
        var response = post(rpc(3, "tools/call", """
                {"name":"list_topics","arguments":{}}"""));

        // then
        assertThatJson(body(response)).inPath("result.isError").isEqualTo(false);
        assertThatJson(body(response)).inPath("result.content[0].text").isString()
                .satisfies(text -> assertThatJson(text).isEqualTo("[{\"type\":\"physical\",\"name\":\"orders\"}]"));
        assertThat(topicsCaller.get())
                .withFailMessage(() -> "GET /topics was not called as the MCP caller but as " + topicsCaller.get())
                .isEqualTo(ALICE);
    }

    @Test
    void routeErrorBecomesToolError() {
        // given
        router = new Router().get("/topics", _ -> Router.Response.notFound("gone"));
        new KawaMcpServer(router, null);

        // when
        var response = post(rpc(4, "tools/call", """
                {"name":"list_topics","arguments":{}}"""));

        // then
        assertThatJson(body(response)).inPath("result.isError").isEqualTo(true);
    }

    @Test
    void notificationIsAccepted() {
        // given
        new KawaMcpServer(router, null);

        // when
        var response = post("""
                {"jsonrpc":"2.0","method":"notifications/initialized"}""");

        // then
        assertThat(response.status())
                .withFailMessage(() -> "notification was not answered with 202 but " + response.status())
                .isEqualTo(202);
    }

    @Test
    void rejectsNonJsonContentType() {
        // given
        new KawaMcpServer(router, null);

        // when
        var response = handle(request(rpc(5, "tools/list", "{}"), Map.of("content-type", "text/plain")));

        // then
        assertThat(response.status())
                .withFailMessage(() -> "non-JSON body was not refused with 415 but " + response.status())
                .isEqualTo(415);
    }

    @Test
    void rejectsMissingAcceptHeader() {
        // given
        new KawaMcpServer(router, null);

        // when
        var response = handle(request(rpc(6, "tools/list", "{}"), Map.of("accept", "application/json")));

        // then
        assertThat(response.status())
                .withFailMessage(() -> "Accept without text/event-stream was not refused with 400 but "
                        + response.status())
                .isEqualTo(400);
    }

    @Test
    void rejectsOriginNotAllowedByCors() {
        // given
        new KawaMcpServer(router, CorsConfig.of(List.of("https://ui.example"), null, null, null, null));

        // when
        var response = handle(request(rpc(7, "tools/list", "{}"), Map.of("origin", "https://evil.example")));

        // then
        assertThat(response.status())
                .withFailMessage(() -> "foreign Origin was not refused with 403 but " + response.status())
                .isEqualTo(403);
    }

    @Test
    void allowsOriginAllowedByCors() {
        // given
        new KawaMcpServer(router, CorsConfig.of(List.of("https://ui.example"), null, null, null, null));

        // when
        var response = handle(request(rpc(8, "tools/list", "{}"), Map.of("origin", "https://ui.example")));

        // then
        assertThat(response.status())
                .withFailMessage(() -> "allowed Origin was not answered with 200 but " + response.status())
                .isEqualTo(200);
    }

    @Test
    void rejectsMalformedJson() {
        // given
        new KawaMcpServer(router, null);

        // when
        var response = post("{not json");

        // then
        assertThat(response.status())
                .withFailMessage(() -> "malformed JSON was not refused with 400 but " + response.status())
                .isEqualTo(400);
        assertThatJson(body(response)).inPath("error.code").isEqualTo(-32600);
    }

    private static String rpc(int id, String method, String params) {
        return "{\"jsonrpc\":\"2.0\",\"id\":" + id + ",\"method\":\"" + method + "\",\"params\":" + params + "}";
    }

    private Router.Response<?> post(String body) {
        return handle(request(body, Map.of()));
    }

    private Router.Response<?> handle(Router.Request request) {
        Router.Match match = router.find(HttpMethod.POST, KawaMcpServer.PATH)
                .orElseThrow(() -> new AssertionError("POST /mcp was not registered"));
        return match.handler().handle(request);
    }

    private static Router.Request request(String body, Map<String, String> headerOverrides) {
        var headers = new HashMap<String, List<String>>();
        headers.put("content-type", List.of("application/json"));
        headers.put("accept", List.of("application/json, text/event-stream"));
        headerOverrides.forEach((name, value) -> headers.put(name, List.of(value)));
        return new Router.Request("POST", KawaMcpServer.PATH, Map.of(), Map.of(), Map.copyOf(headers),
                body.getBytes(StandardCharsets.UTF_8), ALICE);
    }

    private static String body(Router.Response<?> response) {
        return (String) response.body();
    }
}

package io.jonasg.kawa.http;

import io.jonasg.kawa.http.auth.none.AnonymousAuthenticationProvider;
import io.jonasg.kawa.plugin.auth.AuthenticationProvider;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static org.assertj.core.api.Assertions.assertThat;

class RouterTest {

    private static final AuthenticationProvider ANONYMOUS = new AnonymousAuthenticationProvider();

    private static final String JSON = "application/json";

    @Test
    void routesGetToRegisteredHandler() {
        // given
        var router = new Router().get("/topics", request -> Router.Response.ok(List.of()));
        var handler = new HttpRouterHandler(router, ANONYMOUS);

        // when
        FullHttpResponse response = request(handler, HttpMethod.GET, "/topics");

        // then
        assertThat(response.status()).isEqualTo(HttpResponseStatus.OK);
        assertThat(response.headers().get("Content-Type")).isEqualTo(JSON);
        assertThatJson(response.content().toString(StandardCharsets.UTF_8)).isEqualTo("[]");
    }

    @Test
    void returnsNotFoundForUnknownPath() {
        // given
        var handler = new HttpRouterHandler(new Router().get("/topics", request -> Router.Response.ok(List.of())), ANONYMOUS);

        // when
        FullHttpResponse response = request(handler, HttpMethod.GET, "/unknown");

        // then
        assertThat(response.status()).isEqualTo(HttpResponseStatus.NOT_FOUND);
    }

    @Test
    void returnsMethodNotAllowedForKnownPathWithWrongMethod() {
        // given
        var handler = new HttpRouterHandler(new Router().get("/topics", request -> Router.Response.ok(List.of())), ANONYMOUS);

        // when
        FullHttpResponse response = request(handler, HttpMethod.POST, "/topics");

        // then
        assertThat(response.status()).isEqualTo(HttpResponseStatus.METHOD_NOT_ALLOWED);
    }

    @Test
    void setsKeepAliveHeaderWhenRequestIsKeepAlive() {
        // given
        var handler = new HttpRouterHandler(new Router().get("/topics", request -> Router.Response.ok(List.of())), ANONYMOUS);

        // when
        FullHttpResponse response = request(handler, HttpMethod.GET, "/topics");

        // then
        assertThat(response.headers().get("Connection")).isEqualTo("keep-alive");
    }

    @Test
    void passesRequestBodyToHandler() {
        // given
        var router = new Router().put("/configs/rbac", request ->
                Router.Response.ok(new String(request.body(), StandardCharsets.UTF_8)));
        var handler = new HttpRouterHandler(router, ANONYMOUS);

        // when
        FullHttpResponse response = request(handler, HttpMethod.PUT, "/configs/rbac", "{\"roles\":{}}");

        // then
        assertThat(response.status()).isEqualTo(HttpResponseStatus.OK);
        assertThatJson(response.content().toString(StandardCharsets.UTF_8))
                .isEqualTo("\"{\\\"roles\\\":{}}\"");
    }

    @Test
    void writesHandlerStatusAndBody() {
        // given
        var router = new Router().get("/configs/rbac", request -> Router.Response.badRequest("nope"));
        var handler = new HttpRouterHandler(router, ANONYMOUS);

        // when
        FullHttpResponse response = request(handler, HttpMethod.GET, "/configs/rbac");

        // then
        assertThat(response.status()).isEqualTo(HttpResponseStatus.BAD_REQUEST);
        assertThatJson(response.content().toString(StandardCharsets.UTF_8))
                .isEqualTo("{\"error\":\"nope\"}");
    }

    @Test
    void routesPostToRegisteredHandler() {
        // given
        var router = new Router().post("/topics", request -> Router.Response.ok(List.of()));
        var handler = new HttpRouterHandler(router, ANONYMOUS);

        // when
        FullHttpResponse response = request(handler, HttpMethod.POST, "/topics");

        // then
        assertThat(response.status()).isEqualTo(HttpResponseStatus.OK);
    }

    @Test
    void returnsMethodNotAllowedForGetOnPostOnlyPath() {
        // given
        var handler = new HttpRouterHandler(new Router().post("/topics", request -> Router.Response.ok(List.of())), ANONYMOUS);

        // when
        FullHttpResponse response = request(handler, HttpMethod.GET, "/topics");

        // then
        assertThat(response.status()).isEqualTo(HttpResponseStatus.METHOD_NOT_ALLOWED);
    }

    @Test
    void capturesPathParameters() {
        // given
        var router = new Router().delete("/configs/virtual-topics/{name}", request ->
                Router.Response.ok(request.pathParams().get("name")));
        var handler = new HttpRouterHandler(router, ANONYMOUS);

        // when
        FullHttpResponse response = request(handler, HttpMethod.DELETE, "/configs/virtual-topics/orders");

        // then
        assertThat(response.status()).isEqualTo(HttpResponseStatus.OK);
        assertThatJson(response.content().toString(StandardCharsets.UTF_8)).isEqualTo("\"orders\"");
    }

    @Test
    void routesPatchToRegisteredHandler() {
        // given
        var router = new Router().patch("/auth/users/{name}", request ->
                Router.Response.ok(request.pathParams().get("name")));
        var handler = new HttpRouterHandler(router, ANONYMOUS);

        // when
        FullHttpResponse response = request(handler, HttpMethod.PATCH, "/auth/users/alice");

        // then
        assertThat(response.status()).isEqualTo(HttpResponseStatus.OK);
        assertThatJson(response.content().toString(StandardCharsets.UTF_8)).isEqualTo("\"alice\"");
    }

    @Test
    void writesNoContentWithoutBody() {
        // given
        var router = new Router().delete("/configs/virtual-topics/{name}", request -> Router.Response.noContent());
        var handler = new HttpRouterHandler(router, ANONYMOUS);

        // when
        FullHttpResponse response = request(handler, HttpMethod.DELETE, "/configs/virtual-topics/orders");

        // then
        assertThat(response.status()).isEqualTo(HttpResponseStatus.NO_CONTENT);
        assertThat(response.content().readableBytes()).isZero();
    }

    private static FullHttpResponse request(HttpRouterHandler handler, HttpMethod method, String path) {
        return request(handler, method, path, "");
    }

    private static FullHttpResponse request(HttpRouterHandler handler, HttpMethod method, String path, String body) {
        var channel = new EmbeddedChannel(handler);
        FullHttpRequest request = new DefaultFullHttpRequest(
                HttpVersion.HTTP_1_1, method, path, Unpooled.copiedBuffer(body, StandardCharsets.UTF_8));
        channel.writeInbound(request);
        FullHttpResponse response = channel.readOutbound();
        channel.finish();
        return response;
    }
}

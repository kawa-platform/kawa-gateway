package io.jonasg.kawa.http;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import org.jspecify.annotations.Nullable;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

/// Http Routing capable Netty Handler. It decodes a [FullHttpRequest], routes it
/// through the [Router] to a plain handler, and writes the response.
/// All HTTP plumbing lives here so route handlers stay Netty-free.
public final class HttpRouterHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    private static final String JSON = "application/json";

    private final Router router;
    private final JsonMapper mapper;

    public HttpRouterHandler(Router router) {
        this.router = router;
        this.mapper = JsonMapper.builder().build();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
        String uri = request.uri();
        int queryStart = uri.indexOf('?');
        String path = queryStart >= 0 ? uri.substring(0, queryStart) : uri;
        if (!router.hasPath(path)) {
            write(ctx, request, HttpResponseStatus.NOT_FOUND, "{\"error\":\"not found\"}");
            return;
        }
        Router.Match match = router.find(request.method(), path).orElse(null);
        if (match == null) {
            write(ctx, request, HttpResponseStatus.METHOD_NOT_ALLOWED, "{\"error\":\"method not allowed\"}");
            return;
        }
        byte[] body = new byte[request.content().readableBytes()];
        request.content().readBytes(body);
        Router.Request req = new Router.Request(
                request.method().name(),
                path,
                match.pathParams(),
                parseQueryParams(queryStart >= 0 ? uri.substring(queryStart + 1) : ""),
                body);
        Router.Response<?> response;
        try {
            response = match.handler().handle(req);
        } catch (Exception e) {
            write(ctx, request, HttpResponseStatus.INTERNAL_SERVER_ERROR, "{\"error\":\"handler failed\"}");
            return;
        }
        byte[] responseBody;
        if (response.rawBody()) {
            responseBody = ((String) response.body()).getBytes(StandardCharsets.UTF_8);
        } else {
            try {
                responseBody = response.body() == null
                        ? new byte[0]
                        : mapper.writeValueAsBytes(response.body());
            } catch (Exception e) {
                write(ctx, request, HttpResponseStatus.INTERNAL_SERVER_ERROR, "{\"error\":\"serialization failed\"}");
                return;
            }
        }
        write(ctx, request, HttpResponseStatus.valueOf(response.status()), responseBody, response.contentType());
    }

    private static Map<String, String> parseQueryParams(@Nullable String query) {
        if (query == null || query.isBlank()) {
            return Map.of();
        }
        var params = new LinkedHashMap<String, String>();
        Arrays.stream(query.split("&"))
                .filter(part -> !part.isBlank())
                .forEach(part -> {
                    int idx = part.indexOf('=');
                    if (idx < 0) {
                        params.put(part, "");
                        return;
                    }
                    params.put(part.substring(0, idx), part.substring(idx + 1));
                });
        return Map.copyOf(params);
    }

    private static void write(
            ChannelHandlerContext ctx,
            FullHttpRequest request,
            HttpResponseStatus status,
            String body
    ) {
        write(ctx, request, status, body.getBytes(StandardCharsets.UTF_8), JSON);
    }

    private static void write(
            ChannelHandlerContext ctx,
            FullHttpRequest request,
            HttpResponseStatus status,
            byte[] body,
            String contentType
    ) {
        FullHttpResponse response = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1, status, Unpooled.wrappedBuffer(body));
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, contentType);
        response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, body.length);
        boolean keepAlive = HttpUtil.isKeepAlive(request);
        if (keepAlive) {
            response.headers().set(HttpHeaderNames.CONNECTION, "keep-alive");
        }
        ctx.writeAndFlush(response).addListener(_ -> {
            if (!keepAlive) {
                ctx.close();
            }
        });
    }
}

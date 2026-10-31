package io.jonasg.kawa.http.web;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/// Serves the rendered OpenAPI documentation from the classpath resource `/docs/index.html`.
/// The HTML is generated at build time from `openapi.yaml` by the openapi-generator-maven-plugin,
/// so no OpenAPI library is needed at runtime.
public final class GetDocsHandler implements Router.Handler {

    private static final String HTML = "text/html";

    @Override
    public Router.Response<?> handle(Router.Request request) {
        try (InputStream in = GetDocsHandler.class.getResourceAsStream("/docs/index.html")) {
            if (in == null) {
                return Router.Response.internalError("OpenAPI docs resource not found");
            }
            String html = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            return Router.Response.raw(200, html, HTML);
        } catch (IOException e) {
            return Router.Response.internalError("Failed to read OpenAPI docs: " + e.getMessage());
        }
    }
}

package io.jonasg.kawa.http.authentication.oauth;

import com.nimbusds.jose.util.JSONObjectUtils;
import io.jonasg.kawa.plugin.http.HttpEndpoint;
import io.jonasg.kawa.plugin.http.HttpRequest;
import io.jonasg.kawa.plugin.http.HttpResponse;

import java.util.LinkedHashMap;
import java.util.List;

/// `GET /.well-known/oauth-protected-resource[/mcp]`: RFC 9728 metadata describing an OAuth protected
/// resource: the admin API at the root path, the MCP endpoint at the `/mcp` path suffix. It names
/// kawa's built-in authorization server and that tokens go in the `Authorization` header, so a client
/// that got a `401` can discover where to obtain a token on its own (as MCP clients do). The resource
/// is the URL the client was given (the admin API's public URL, or `<publicUrl>/mcp`), which strict
/// MCP clients check; the authorization server is named by its issuer, so a client can match it
/// against the `issuer` of the authorization server metadata.
final class ProtectedResourceMetadataEndpoint implements HttpEndpoint {

    static final String PATH = "/.well-known/oauth-protected-resource";

    private final String metadata;

    /// @param resource the protected resource's URL, served as `resource`
    ProtectedResourceMetadataEndpoint(String resource, String issuer) {
        var document = new LinkedHashMap<String, Object>();
        document.put("resource", resource);
        document.put("authorization_servers", List.of(issuer));
        document.put("bearer_methods_supported", List.of("header"));
        this.metadata = JSONObjectUtils.toJSONString(document);
    }

    @Override
    public HttpResponse handle(HttpRequest request) {
        return HttpResponse.json(200, metadata);
    }
}

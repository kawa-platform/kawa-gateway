package io.jonasg.kawa.http.authentication.oauth;

import com.nimbusds.jose.util.JSONObjectUtils;
import io.jonasg.kawa.plugin.http.HttpEndpoint;
import io.jonasg.kawa.plugin.http.HttpRequest;
import io.jonasg.kawa.plugin.http.HttpResponse;

import java.util.LinkedHashMap;
import java.util.List;

/// `GET /.well-known/oauth-protected-resource`: RFC 9728 metadata describing the admin API as an
/// OAuth protected resource. It names kawa's built-in authorization server and that tokens go in the
/// `Authorization` header, so a client that got a `401` can discover where to obtain a token on its
/// own (as MCP clients do). Both the resource and the authorization server are the admin API's public
/// URL.
final class ProtectedResourceMetadataEndpoint implements HttpEndpoint {

    static final String PATH = "/.well-known/oauth-protected-resource";

    private final String metadata;

    ProtectedResourceMetadataEndpoint(String publicUrl) {
        var document = new LinkedHashMap<String, Object>();
        document.put("resource", publicUrl);
        document.put("authorization_servers", List.of(publicUrl));
        document.put("bearer_methods_supported", List.of("header"));
        this.metadata = JSONObjectUtils.toJSONString(document);
    }

    @Override
    public HttpResponse handle(HttpRequest request) {
        return HttpResponse.json(200, metadata);
    }
}

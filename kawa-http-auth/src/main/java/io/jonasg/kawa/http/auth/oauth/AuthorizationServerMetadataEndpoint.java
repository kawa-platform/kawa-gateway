package io.jonasg.kawa.http.auth.oauth;

import com.nimbusds.oauth2.sdk.GrantType;
import com.nimbusds.oauth2.sdk.ResponseType;
import com.nimbusds.oauth2.sdk.as.AuthorizationServerMetadata;
import com.nimbusds.oauth2.sdk.auth.ClientAuthenticationMethod;
import com.nimbusds.oauth2.sdk.id.Issuer;
import com.nimbusds.oauth2.sdk.pkce.CodeChallengeMethod;
import io.jonasg.kawa.plugin.http.HttpEndpoint;
import io.jonasg.kawa.plugin.http.HttpRequest;
import io.jonasg.kawa.plugin.http.HttpResponse;

import java.net.URI;
import java.util.List;

/// `GET /.well-known/oauth-authorization-server`: RFC 8414 metadata describing kawa's built-in
/// authorization server (issuer, authorization and token endpoints, JWKS location, the authorization
/// code flow with PKCE S256 and the refresh token grant). Endpoint URLs
/// are derived from the request's `Host` header (and `X-Forwarded-Proto`, when kawa sits behind a
/// TLS-terminating proxy).
final class AuthorizationServerMetadataEndpoint implements HttpEndpoint {

    private final String issuer;

    AuthorizationServerMetadataEndpoint(String issuer) {
        this.issuer = issuer;
    }

    @Override
    public HttpResponse handle(HttpRequest request) {
        String scheme = request.header("x-forwarded-proto").orElse("http");
        String host = request.header("host").orElse("localhost");
        var metadata = new AuthorizationServerMetadata(new Issuer(issuer));
        metadata.setAuthorizationEndpointURI(URI.create(scheme + "://" + host + "/oauth/authorize"));
        metadata.setTokenEndpointURI(URI.create(scheme + "://" + host + "/oauth/token"));
        metadata.setJWKSetURI(URI.create(scheme + "://" + host + "/.well-known/jwks.json"));
        metadata.setResponseTypes(List.of(ResponseType.CODE));
        metadata.setGrantTypes(List.of(GrantType.AUTHORIZATION_CODE, GrantType.REFRESH_TOKEN));
        metadata.setCodeChallengeMethods(List.of(CodeChallengeMethod.S256));
        metadata.setTokenEndpointAuthMethods(List.of(ClientAuthenticationMethod.NONE));
        return HttpResponse.json(200, metadata.toJSONObject().toJSONString());
    }
}

package io.jonasg.kawa.http.authentication.oauth;

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
import java.util.function.BooleanSupplier;

/// `GET /.well-known/oauth-authorization-server`: RFC 8414 metadata describing kawa's built-in
/// authorization server (issuer, authorization and token endpoints, JWKS location, the authorization
/// code flow with PKCE S256, the refresh token grant, and the client credentials and token exchange
/// grants for service clients), and, while client ID metadata documents are enabled, `client_id_metadata_document_supported`
/// (OAuth Client ID Metadata Documents), decided on every request. Endpoint URLs are built from the
/// admin API's public URL (`admin.publicUrl`), never from request headers, so a client that discovers
/// kawa is always sent to the address the operator configured.
final class AuthorizationServerMetadataEndpoint implements HttpEndpoint {

    private final String issuer;
    private final String publicUrl;
    private final BooleanSupplier clientIdMetadataDocuments;

    /// @param publicUrl                 the admin API's public URL, without a trailing `/`, that the endpoint URLs are built from
    /// @param clientIdMetadataDocuments whether clients may identify themselves with a metadata document right now
    AuthorizationServerMetadataEndpoint(String issuer, String publicUrl, BooleanSupplier clientIdMetadataDocuments) {
        this.issuer = issuer;
        this.publicUrl = publicUrl;
        this.clientIdMetadataDocuments = clientIdMetadataDocuments;
    }

    @Override
    public HttpResponse handle(HttpRequest request) {
        var metadata = new AuthorizationServerMetadata(new Issuer(issuer));
        metadata.setAuthorizationEndpointURI(URI.create(publicUrl + "/oauth/authorize"));
        metadata.setTokenEndpointURI(URI.create(publicUrl + "/oauth/token"));
        metadata.setJWKSetURI(URI.create(publicUrl + "/.well-known/jwks.json"));
        metadata.setResponseTypes(List.of(ResponseType.CODE));
        metadata.setGrantTypes(List.of(GrantType.AUTHORIZATION_CODE, GrantType.REFRESH_TOKEN, GrantType.CLIENT_CREDENTIALS, GrantType.TOKEN_EXCHANGE));
        metadata.setCodeChallengeMethods(List.of(CodeChallengeMethod.S256));
        metadata.setTokenEndpointAuthMethods(List.of(
                ClientAuthenticationMethod.NONE,
                ClientAuthenticationMethod.CLIENT_SECRET_BASIC,
                ClientAuthenticationMethod.CLIENT_SECRET_POST));
        if (clientIdMetadataDocuments.getAsBoolean()) {
            metadata.setCustomParameter("client_id_metadata_document_supported", true);
        }
        return HttpResponse.json(200, metadata.toJSONObject().toJSONString());
    }
}

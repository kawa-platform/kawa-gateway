package io.jonasg.kawa.it;

import io.jonasg.kawa.config.AdminAuthConfig;
import io.jonasg.kawa.config.AdminConfig;
import io.modelcontextprotocol.client.McpClient;
import io.modelcontextprotocol.client.McpSyncClient;
import io.modelcontextprotocol.client.transport.HttpClientStreamableHttpTransport;
import io.modelcontextprotocol.spec.McpSchema;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static org.assertj.core.api.Assertions.assertThat;

/// End-to-end OAuth on the admin API of a real gateway: kawa seeds the bootstrap user into the config
/// topic, issues tokens for the admin users stored there, and rejects requests without one.
///
/// The admin port is picked up front so `admin.publicUrl` can name it: kawa publishes that URL as issuer
/// and in its discovery metadata, and an ephemeral port would only be known after binding.
class AdminOAuthIT extends AdminHTTPTestSupport {

    private static final String STUDIO_CLIENT_ID = "kawa-studio";
    private static final String REDIRECT_URI = "http://localhost:5173/admin/auth/callback";
    private static final String MCP_CLIENT_ID = "kawa-mcp-claude";
    private static final int ADMIN_PORT = freePort();
    private static final String PUBLIC_URL = "http://127.0.0.1:" + ADMIN_PORT;
    private static final String MCP_RESOURCE = PUBLIC_URL + "/mcp";
    private static final Pattern RESOURCE_METADATA = Pattern.compile("resource_metadata=\"([^\"]+)\"");
    private static final JsonMapper JSON = JsonMapper.builder().build();
    /// The `oauth` provider fetches client documents from public https hosts only; this system property lets the test serve one
    /// from a loopback port. Set before the gateway starts, it is a JVM flag a deployment's config cannot set.
    private static final String CIMD_LOOPBACK_PROPERTY = "kawa.test.clientIdMetadataLoopback";
    private static final HttpServer CLIENT_DOCUMENTS = clientDocumentServer();
    private static final String CIMD_CLIENT_ID = "http://127.0.0.1:" + CLIENT_DOCUMENTS.getAddress().getPort() + "/client.json";
    private static final String CIMD_PRIVATE_CLIENT_ID = "https://10.255.255.1/client.json";

    static {
        System.setProperty(CIMD_LOOPBACK_PROPERTY, "true");
    }

    @Override
    protected AdminConfig adminConfig() {
        return new AdminConfig(true, "127.0.0.1", ADMIN_PORT, null,
                new AdminAuthConfig("oauth", new AdminAuthConfig.BootstrapUser("admin@kawa", "s3cret"), Map.of(
                        "loginUrl", "http://localhost:5173/admin/login",
                        "redirectUris", REDIRECT_URI)),
                PUBLIC_URL);
    }

    @AfterAll
    static void stopClientDocuments() {
        CLIENT_DOCUMENTS.stop(0);
        System.clearProperty(CIMD_LOOPBACK_PROPERTY);
    }

    /// Leaves the `mcp-cimd` module on, as it is by default, whatever a test did to it.
    @AfterEach
    void switchClientIdMetadataDocumentsOn() throws Exception {
        switchClientIdMetadataDocuments(true);
    }

    /// Removes the OAuth client a test created, so no test depends on another's leftovers.
    @AfterEach
    void deleteMcpClient() throws Exception {
        HttpResponse<String> deleteClientResp = httpExec(reqBuilder("/admin/oauth-clients/" + MCP_CLIENT_ID)
                .header("Authorization", "Bearer " + login("admin@kawa", "s3cret"))
                .DELETE().build());
        assertThat(deleteClientResp.statusCode())
                .withFailMessage(() -> "OAuth client '" + MCP_CLIENT_ID + "' was not cleaned up: " + deleteClientResp.body())
                .isIn(200, 204, 404);
    }

    @Test
    void mcpClientDiscoversTheAuthorizationServerAndCallsATool() throws Exception {
        // given: the discovered authorization server metadata
        JsonNode serverMetadata = discoverAuthorizationServer();

        // and: the pre-registered client obtains a token from the advertised endpoints
        String token = signIn("admin@kawa", "s3cret", serverMetadata.get("authorization_endpoint").asString(),
                serverMetadata.get("token_endpoint").asString()).get("access_token").asString();

        // when
        McpSchema.CallToolResult result = listTopicsOverMcp(token);

        // then
        assertThat(result.isError())
                .withFailMessage(() -> "list_topics with a discovered token failed: " + result.content())
                .isFalse();
        assertThatJson(result.structuredContent())
                .withFailMessage(() -> "list_topics returned no topic listing: " + result.structuredContent())
                .isObject().containsKeys("virtualTopics", "physicalTopics");
    }

    @Test
    void storedMcpClientSignsInOnARandomLoopbackPortAndCallsATool() throws Exception {
        // given: an admin registers an MCP client that accepts any port on localhost
        String adminToken = login("admin@kawa", "s3cret");
        HttpResponse<String> putClientResp = putMcpClient(adminToken, true);
        assertThat(putClientResp.statusCode())
                .withFailMessage(() -> "PUT /admin/oauth-clients/" + MCP_CLIENT_ID + " was not accepted: " + putClientResp.body())
                .isEqualTo(200);
        String redirectUri = "http://localhost:" + freePort() + "/callback";
        awaitAuthorizable(MCP_CLIENT_ID, redirectUri);

        // and: the client discovers the authorization server from the MCP endpoint
        JsonNode serverMetadata = discoverAuthorizationServer();

        // when: it signs in with the MCP endpoint as resource
        JsonNode tokens = signIn("admin@kawa", "s3cret", MCP_CLIENT_ID, redirectUri, MCP_RESOURCE,
                serverMetadata.get("authorization_endpoint").asString(), serverMetadata.get("token_endpoint").asString());
        String token = tokens.get("access_token").asString();

        // then: the token is for the MCP endpoint and names the client
        JsonNode claims = claims(token);
        assertThat(claims.get("aud").isArray() ? claims.get("aud").get(0).asString() : claims.get("aud").asString())
                .withFailMessage(() -> "the token's audience is not the MCP endpoint: " + claims)
                .isEqualTo(MCP_RESOURCE);
        assertThat(claims.get("client_id").asString())
                .withFailMessage(() -> "the token does not name client '" + MCP_CLIENT_ID + "': " + claims)
                .isEqualTo(MCP_CLIENT_ID);

        // and: it calls an MCP tool
        McpSchema.CallToolResult result = listTopicsOverMcp(token);
        assertThat(result.isError())
                .withFailMessage(() -> "list_topics with the MCP client's token failed: " + result.content())
                .isFalse();

        // and: it is refused on the rest of the admin API
        HttpResponse<String> topicsResp = httpExec(reqBuilder("/topics").GET()
                .header("Authorization", "Bearer " + token).build());
        assertThat(topicsResp.statusCode())
                .withFailMessage(() -> "GET /topics with an MCP-audience token was not rejected: " + topicsResp.statusCode())
                .isEqualTo(401);
    }

    @Test
    void aClientIdentifiedByAMetadataDocumentSignsInAndCallsATool() throws Exception {
        // given: nothing registers the client; CIMD is on by default and published in the discovery metadata
        JsonNode serverMetadata = discoverAuthorizationServer();
        assertThat(serverMetadata.get("client_id_metadata_document_supported").asBoolean())
                .withFailMessage(() -> "the metadata does not advertise CIMD support: " + serverMetadata)
                .isTrue();
        String redirectUri = "http://localhost:" + freePort() + "/callback";
        awaitAuthorizable(CIMD_CLIENT_ID, redirectUri);

        // when: it signs in with its URL as client_id and the MCP endpoint as resource
        JsonNode tokens = signIn("admin@kawa", "s3cret", CIMD_CLIENT_ID, redirectUri, MCP_RESOURCE,
                serverMetadata.get("authorization_endpoint").asString(), serverMetadata.get("token_endpoint").asString());
        String token = tokens.get("access_token").asString();

        // then: the token is for the MCP endpoint and names the client
        JsonNode claims = claims(token);
        assertThat(claims.get("client_id").asString())
                .withFailMessage(() -> "the token does not name the CIMD client: " + claims)
                .isEqualTo(CIMD_CLIENT_ID);
        McpSchema.CallToolResult result = listTopicsOverMcp(token);
        assertThat(result.isError())
                .withFailMessage(() -> "list_topics with the CIMD client's token failed: " + result.content())
                .isFalse();

        // and: the login UI can see who is asking
        String challenge = queryParam(authorize(CIMD_CLIENT_ID, redirectUri, MCP_RESOURCE, "challenge-" + UUID.randomUUID())
                .headers().firstValue("Location").orElseThrow(), "login_challenge");
        JsonNode asking = getJson(PUBLIC_URL + "/oauth/login-challenge?login_challenge=" + encode(challenge));
        assertThat(asking.get("clientName").asString()).withFailMessage(() -> "wrong client name: " + asking).isEqualTo("IT client");
        assertThat(asking.get("registered").asBoolean()).withFailMessage(() -> "a CIMD client was shown as registered: " + asking).isFalse();
        assertThat(asking.get("audience").asString()).isEqualTo("mcp");

        // when: the admin switches CIMD off
        switchClientIdMetadataDocuments(false);

        // then: the token stops working, no new sign-in starts and the metadata stops advertising it
        eventually(() -> {
            assertThat(mcpToolsList(token).statusCode())
                    .withFailMessage(() -> "a CIMD token still reached /mcp after CIMD was switched off")
                    .isEqualTo(401);
            assertThat(authorize(CIMD_CLIENT_ID, redirectUri, MCP_RESOURCE, "challenge-" + UUID.randomUUID()).statusCode())
                    .withFailMessage(() -> "a CIMD sign-in started after CIMD was switched off")
                    .isEqualTo(400);
            assertThat(discoverAuthorizationServer().has("client_id_metadata_document_supported"))
                    .withFailMessage(() -> "the metadata still advertises CIMD after it was switched off")
                    .isFalse();
        });

        // and: switching it on again brings the client back
        switchClientIdMetadataDocuments(true);
        eventually(() -> assertThat(mcpToolsList(token).statusCode())
                .withFailMessage(() -> "the CIMD token did not work again after CIMD was switched back on")
                .isEqualTo(200));
    }

    @Test
    void refusesAMetadataDocumentUrlOnAnAddressItMayNotFetchFrom() throws Exception {
        // when: the URL's host is a private address (the test policy only fetches from loopback)
        HttpResponse<String> response = authorize(CIMD_PRIVATE_CLIENT_ID, "http://localhost:" + freePort() + "/callback", null,
                "challenge-" + UUID.randomUUID());

        // then
        assertThat(response.statusCode())
                .withFailMessage(() -> "a client_id URL on a private address was not refused: " + response.statusCode() + " " + response.body())
                .isEqualTo(400);
    }

    @Test
    void refusesRedirectUrisAndClientsThatAreNotRegistered() throws Exception {
        // given
        HttpResponse<String> putClientResp = putMcpClient(login("admin@kawa", "s3cret"), true);
        assertThat(putClientResp.statusCode())
                .withFailMessage(() -> "PUT /admin/oauth-clients/" + MCP_CLIENT_ID + " was not accepted: " + putClientResp.body())
                .isEqualTo(200);
        int port = freePort();
        awaitAuthorizable(MCP_CLIENT_ID, "http://localhost:" + port + "/callback");

        for (String[] refused : new String[][] {
                {MCP_CLIENT_ID, "http://localhost:" + port + "/other"},
                {MCP_CLIENT_ID, "https://localhost:" + port + "/callback"},
                {"unknown-client", "http://localhost:" + port + "/callback"}}) {
            String clientId = refused[0];
            String redirectUri = refused[1];

            // when
            HttpResponse<String> authorizeResp = authorize(clientId, redirectUri, null, "challenge-" + UUID.randomUUID());

            // then
            assertThat(authorizeResp.statusCode())
                    .withFailMessage(() -> "authorize for client '" + clientId + "' with redirect " + redirectUri
                            + " was not refused with 400: " + authorizeResp.statusCode() + " " + authorizeResp.body())
                    .isEqualTo(400);
            assertThat(authorizeResp.headers().firstValue("Location"))
                    .withFailMessage(() -> "authorize for client '" + clientId + "' with redirect " + redirectUri
                            + " redirected: " + authorizeResp.headers().firstValue("Location"))
                    .isEmpty();
        }
    }

    @Test
    void disablingOrDeletingAnOAuthClientRevokesItsTokens() throws Exception {
        // given: a signed-in MCP client
        String adminToken = login("admin@kawa", "s3cret");
        HttpResponse<String> putClientResp = putMcpClient(adminToken, true);
        assertThat(putClientResp.statusCode())
                .withFailMessage(() -> "PUT /admin/oauth-clients/" + MCP_CLIENT_ID + " was not accepted: " + putClientResp.body())
                .isEqualTo(200);
        String redirectUri = "http://localhost:" + freePort() + "/callback";
        awaitAuthorizable(MCP_CLIENT_ID, redirectUri);
        JsonNode tokens = signIn("admin@kawa", "s3cret", MCP_CLIENT_ID, redirectUri, MCP_RESOURCE,
                baseUrl() + "/oauth/authorize", baseUrl() + "/oauth/token");
        String accessToken = tokens.get("access_token").asString();
        String refreshToken = tokens.get("refresh_token").asString();
        assertThat(mcpToolsList(accessToken).statusCode())
                .withFailMessage(() -> "the MCP client's token was not accepted on /mcp before disabling it")
                .isEqualTo(200);

        // when: the client is disabled
        HttpResponse<String> disableClientResp = putMcpClient(adminToken, false);

        // then: its access and refresh tokens are refused
        assertThat(disableClientResp.statusCode())
                .withFailMessage(() -> "disabling OAuth client '" + MCP_CLIENT_ID + "' was not accepted: " + disableClientResp.body())
                .isEqualTo(200);
        eventually(() -> assertThat(mcpToolsList(accessToken).statusCode())
                .withFailMessage(() -> "the access token of disabled client '" + MCP_CLIENT_ID + "' was not rejected on /mcp")
                .isEqualTo(401));
        eventually(() -> {
            HttpResponse<String> refreshResp = tokenRequest(baseUrl() + "/oauth/token",
                    "grant_type=refresh_token&refresh_token=" + encode(refreshToken));
            assertThat(refreshResp.statusCode())
                    .withFailMessage(() -> "the refresh token of disabled client '" + MCP_CLIENT_ID + "' was not refused: "
                            + refreshResp.statusCode() + " " + refreshResp.body())
                    .isEqualTo(400);
            assertThatJson(refreshResp.body()).inPath("error").isString()
                    .withFailMessage(() -> "the refused refresh is not invalid_grant: " + refreshResp.body())
                    .isEqualTo("invalid_grant");
        });

        // when: the client is enabled again
        HttpResponse<String> enableClientResp = putMcpClient(adminToken, true);

        // then: the unexpired access token works again
        assertThat(enableClientResp.statusCode())
                .withFailMessage(() -> "re-enabling OAuth client '" + MCP_CLIENT_ID + "' was not accepted: " + enableClientResp.body())
                .isEqualTo(200);
        eventually(() -> assertThat(mcpToolsList(accessToken).statusCode())
                .withFailMessage(() -> "the access token of re-enabled client '" + MCP_CLIENT_ID + "' was not accepted on /mcp")
                .isEqualTo(200));

        // when: the client is deleted
        HttpResponse<String> deleteClientResp = httpExec(reqBuilder("/admin/oauth-clients/" + MCP_CLIENT_ID)
                .header("Authorization", "Bearer " + adminToken)
                .DELETE().build());

        // then
        assertThat(deleteClientResp.statusCode())
                .withFailMessage(() -> "deleting OAuth client '" + MCP_CLIENT_ID + "' was not accepted: " + deleteClientResp.body())
                .isIn(200, 204);
        eventually(() -> assertThat(mcpToolsList(accessToken).statusCode())
                .withFailMessage(() -> "the access token of deleted client '" + MCP_CLIENT_ID + "' was not rejected on /mcp")
                .isEqualTo(401));
    }

    @Test
    void studioTokenWorksOnTheAdminApiAndOnMcp() throws Exception {
        // given
        String token = login("admin@kawa", "s3cret");

        // when
        HttpResponse<String> topicsResp = httpExec(reqBuilder("/topics").GET()
                .header("Authorization", "Bearer " + token).build());
        McpSchema.CallToolResult result = listTopicsOverMcp(token);

        // then
        assertThat(topicsResp.statusCode())
                .withFailMessage(() -> "GET /topics with a studio token failed: " + topicsResp.body())
                .isEqualTo(200);
        assertThat(result.isError())
                .withFailMessage(() -> "list_topics with a studio token failed: " + result.content())
                .isFalse();
    }

    @Test
    void rejectsAdminRequestsWithoutToken() throws Exception {
        // when
        HttpResponse<String> response = httpExec(reqBuilder("/topics").GET().build());

        // then
        assertThat(response.statusCode())
                .withFailMessage(() -> "GET /topics without a token was not rejected: " + response.statusCode())
                .isEqualTo(401);
    }

    @Test
    void rejectsMcpRequestsWithoutToken() throws Exception {
        // when
        HttpResponse<String> response = mcpToolsList(null);

        // then
        assertThat(response.statusCode())
                .withFailMessage(() -> "POST /mcp without a token was not rejected: " + response.statusCode())
                .isEqualTo(401);
    }

    @Test
    void bootstrapUserCallsTheAdminApiWithAToken() throws Exception {
        // given
        String token = login("admin@kawa", "s3cret");

        // when
        HttpResponse<String> response = httpExec(reqBuilder("/topics").GET()
                .header("Authorization", "Bearer " + token).build());

        // then
        assertThat(response.statusCode())
                .withFailMessage(() -> "GET /topics with a valid token failed: " + response.body())
                .isEqualTo(200);
    }

    @Test
    void seedsTheBootstrapUserIntoTheConfigTopic() throws Exception {
        // when
        HttpResponse<String> response = httpExec(reqBuilder("/admin/users").GET()
                .header("Authorization", "Bearer " + login("admin@kawa", "s3cret")).build());

        // then
        assertThat(response.statusCode())
                .withFailMessage(() -> "GET /admin/users as the bootstrap user failed: " + response.body())
                .isEqualTo(200);
        assertThat(response.body())
                .withFailMessage(() -> "the bootstrap user is not a stored admin user: " + response.body())
                .contains("\"email\":\"admin@kawa\"");
    }

    @Test
    void adminUserCreatedThroughTheApiCanLogIn() throws Exception {
        // given: the bootstrap admin creates 'alice@mycom.com', persisted in and applied from the config topic
        String adminToken = login("admin@kawa", "s3cret");
        HttpResponse<String> created = httpExec(reqBuilder("/admin/users?consistency=applied")
                .POST(HttpRequest.BodyPublishers.ofString("{\"email\":\"alice@mycom.com\",\"password\":\"alice-secret\"}"))
                .header("Content-Type", "application/json")
                .header("Authorization", "Bearer " + adminToken).build());
        assertThat(created.statusCode())
                .withFailMessage(() -> "creating admin user 'alice@mycom.com' failed: " + created.body())
                .isEqualTo(201);

        // when
        String aliceToken = login("alice@mycom.com", "alice-secret");
        HttpResponse<String> response = httpExec(reqBuilder("/admin/users").GET()
                .header("Authorization", "Bearer " + aliceToken).build());

        // then
        assertThat(response.statusCode())
                .withFailMessage(() -> "GET /admin/users as 'alice@mycom.com' failed: " + response.body())
                .isEqualTo(200);
        assertThat(response.body()).contains("\"alice@mycom.com\"");
    }

    @Test
    void refreshTokenObtainsANewAccessToken() throws Exception {
        // given
        String refreshToken = signIn("admin@kawa", "s3cret").get("refresh_token").asString();

        // when
        String accessToken = tokenResponse("grant_type=refresh_token&refresh_token=" + refreshToken)
                .get("access_token").asString();
        HttpResponse<String> response = httpExec(reqBuilder("/topics").GET()
                .header("Authorization", "Bearer " + accessToken).build());

        // then
        assertThat(response.statusCode())
                .withFailMessage(() -> "GET /topics with a refreshed access token failed: " + response.body())
                .isEqualTo(200);
    }

    /// Walks MCP discovery as a client does: the 401 on `/mcp` points at the MCP endpoint's protected resource
    /// metadata, which names the authorization server, whose metadata is returned.
    private JsonNode discoverAuthorizationServer() throws Exception {
        HttpResponse<String> challenged = mcpToolsList(null);
        assertThat(challenged.statusCode())
                .withFailMessage(() -> "POST /mcp without a token was not rejected: " + challenged.statusCode())
                .isEqualTo(401);
        String challenge = challenged.headers().firstValue("WWW-Authenticate").orElse("");
        Matcher resourceMetadataUrl = RESOURCE_METADATA.matcher(challenge);
        assertThat(resourceMetadataUrl.find())
                .withFailMessage(() -> "the 401 does not point at the protected resource metadata: " + challenge)
                .isTrue();
        assertThat(resourceMetadataUrl.group(1))
                .withFailMessage(() -> "the 401 on /mcp does not point at the MCP endpoint's metadata: " + challenge)
                .isEqualTo(PUBLIC_URL + "/.well-known/oauth-protected-resource/mcp");

        JsonNode resourceMetadata = getJson(resourceMetadataUrl.group(1));
        assertThat(resourceMetadata.get("resource").asString())
                .withFailMessage(() -> "the protected resource is not the MCP endpoint URL: " + resourceMetadata)
                .isEqualTo(MCP_RESOURCE);
        String issuer = resourceMetadata.get("authorization_servers").get(0).asString();

        JsonNode serverMetadata = getJson(issuer + "/.well-known/oauth-authorization-server");
        assertThat(serverMetadata.get("issuer").asString())
                .withFailMessage(() -> "the authorization server metadata does not belong to issuer " + issuer + ": " + serverMetadata)
                .isEqualTo(issuer);
        return serverMetadata;
    }

    private HttpResponse<String> putMcpClient(String adminToken, boolean enabled) throws Exception {
        return httpExec(reqBuilder("/admin/oauth-clients/" + MCP_CLIENT_ID)
                .header("Content-Type", "application/json")
                .header("Authorization", "Bearer " + adminToken)
                .PUT(HttpRequest.BodyPublishers.ofString("""
                        {"name":"Claude Code","enabled":%s,"redirectUris":["http://localhost:*/callback"],"audience":"mcp"}"""
                        .formatted(enabled)))
                .build());
    }

    /// Waits until the gateway has applied the stored client from the config topic: authorize then redirects to
    /// the login UI instead of refusing the client.
    private void awaitAuthorizable(String clientId, String redirectUri) {
        eventually(() -> {
            HttpResponse<String> authorizeResp = authorize(clientId, redirectUri, null, "challenge-" + UUID.randomUUID());
            assertThat(authorizeResp.statusCode())
                    .withFailMessage(() -> "OAuth client '" + clientId + "' was not applied: " + authorizeResp.body())
                    .isEqualTo(302);
        });
    }

    private HttpResponse<String> mcpToolsList(String token) throws Exception {
        HttpRequest.Builder request = reqBuilder("/mcp")
                .header("Content-Type", "application/json")
                .header("Accept", "application/json, text/event-stream")
                .POST(HttpRequest.BodyPublishers.ofString("""
                        {"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}"""));
        if (token != null) {
            request.header("Authorization", "Bearer " + token);
        }
        return httpExec(request.build());
    }

    private static McpSchema.CallToolResult listTopicsOverMcp(String token) {
        McpSyncClient client = McpClient.sync(HttpClientStreamableHttpTransport.builder(PUBLIC_URL).endpoint("/mcp")
                        .httpRequestCustomizer((request, method, uri, body, context) ->
                                request.header("Authorization", "Bearer " + token))
                        .build())
                .requestTimeout(Duration.ofSeconds(10))
                .build();
        try {
            client.initialize();
            return client.callTool(new McpSchema.CallToolRequest("list_topics", Map.of()));
        } finally {
            client.closeGracefully();
        }
    }

    private String login(String username, String password) throws Exception {
        return signIn(username, password).get("access_token").asString();
    }

    private JsonNode signIn(String username, String password) throws Exception {
        return signIn(username, password, baseUrl() + "/oauth/authorize", baseUrl() + "/oauth/token");
    }

    private JsonNode signIn(String username, String password, String authorizeEndpoint, String tokenEndpoint) throws Exception {
        return signIn(username, password, STUDIO_CLIENT_ID, REDIRECT_URI, null, authorizeEndpoint, tokenEndpoint);
    }

    /// The authorization code flow with PKCE, as kawa studio and the browser run it: authorize (redirect
    /// to the login UI with a challenge), log in, then redeem the code. `resource` (RFC 8707) is sent to both
    /// endpoints when given.
    private JsonNode signIn(String username, String password, String clientId, String redirectUri, String resource,
                            String authorizeEndpoint, String tokenEndpoint) throws Exception {
        String verifier = "verifier-" + UUID.randomUUID() + "-" + UUID.randomUUID();
        String challenge = Base64.getUrlEncoder().withoutPadding().encodeToString(
                MessageDigest.getInstance("SHA-256").digest(verifier.getBytes(StandardCharsets.US_ASCII)));
        HttpResponse<String> authorize = authorize(authorizeEndpoint, clientId, redirectUri, resource, challenge);
        String loginChallenge = queryParam(authorize.headers().firstValue("Location")
                .orElseThrow(() -> new AssertionError("authorize did not redirect: " + authorize.body())), "login_challenge");
        HttpResponse<String> login = httpExec(reqBuilder("/oauth/login")
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(Map.of(
                        "login_challenge", loginChallenge, "username", username, "password", password))))
                .build());
        assertThat(login.statusCode())
                .withFailMessage(() -> "login as '" + username + "' failed: " + login.body())
                .isEqualTo(200);
        String redirectTo = JSON.readTree(login.body()).get("redirect_to").asString();
        assertThat(redirectTo)
                .withFailMessage(() -> "login did not redirect to the requested URI " + redirectUri + ": " + redirectTo)
                .startsWith(redirectUri + "?");
        String code = queryParam(redirectTo, "code");
        return tokenResponse(tokenEndpoint, "grant_type=authorization_code&client_id=" + encode(clientId) + "&code=" + encode(code)
                + "&redirect_uri=" + encode(redirectUri) + "&code_verifier=" + verifier
                + (resource == null ? "" : "&resource=" + encode(resource)));
    }

    private HttpResponse<String> authorize(String clientId, String redirectUri, String resource, String challenge) throws Exception {
        return authorize(baseUrl() + "/oauth/authorize", clientId, redirectUri, resource, challenge);
    }

    private HttpResponse<String> authorize(String authorizeEndpoint, String clientId, String redirectUri, String resource,
                                           String challenge) throws Exception {
        return httpExec(HttpRequest.newBuilder(URI.create(authorizeEndpoint
                + "?response_type=code&client_id=" + encode(clientId)
                + "&redirect_uri=" + encode(redirectUri) + "&code_challenge=" + challenge
                + "&code_challenge_method=S256&state=it"
                + (resource == null ? "" : "&resource=" + encode(resource)))).GET().build());
    }

    private static JsonNode claims(String jwt) {
        return JSON.readTree(Base64.getUrlDecoder().decode(jwt.split("\\.")[1]));
    }

    private JsonNode getJson(String url) throws Exception {
        HttpResponse<String> response = httpExec(HttpRequest.newBuilder(URI.create(url)).GET().build());
        assertThat(response.statusCode())
                .withFailMessage(() -> "GET " + url + " failed: " + response.body())
                .isEqualTo(200);
        return JSON.readTree(response.body());
    }

    /// Switches the `mcp-cimd` module, waiting until the gateway applied it.
    private void switchClientIdMetadataDocuments(boolean enabled) throws Exception {
        HttpResponse<String> response = httpExec(reqBuilder("/modules/mcp-cimd?consistency=applied")
                .header("Authorization", "Bearer " + login("admin@kawa", "s3cret"))
                .header("Content-Type", "application/json")
                .PUT(HttpRequest.BodyPublishers.ofString("{\"enabled\":" + enabled + "}"))
                .build());
        assertThat(response.statusCode())
                .withFailMessage(() -> "PUT /modules/mcp-cimd was not accepted: " + response.body())
                .isEqualTo(200);
    }

    /// Serves the metadata document of the CIMD client at `/client.json`.
    private static HttpServer clientDocumentServer() {
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
            for (String path : new String[] {"/client.json"}) {
                server.createContext(path, exchange -> {
                    String clientId = "http://127.0.0.1:" + server.getAddress().getPort() + path;
                    byte[] body = ("{\"client_id\": \"" + clientId + "\", \"client_name\": \"IT client\", "
                            + "\"redirect_uris\": [\"http://localhost:*/callback\"], \"token_endpoint_auth_method\": \"none\"}")
                            .getBytes(StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().add("Content-Type", "application/json");
                    exchange.sendResponseHeaders(200, body.length);
                    exchange.getResponseBody().write(body);
                    exchange.close();
                });
            }
            server.start();
            return server;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static int freePort() {
        try (var socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String queryParam(String url, String name) {
        return Arrays.stream(URI.create(url).getRawQuery().split("&"))
                .map(pair -> pair.split("=", 2))
                .filter(pair -> pair[0].equals(name))
                .map(pair -> URLDecoder.decode(pair[1], StandardCharsets.UTF_8))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no " + name + " in " + url));
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private JsonNode tokenResponse(String form) throws Exception {
        return tokenResponse(baseUrl() + "/oauth/token", form);
    }

    private JsonNode tokenResponse(String tokenEndpoint, String form) throws Exception {
        HttpResponse<String> response = tokenRequest(tokenEndpoint, form);
        assertThat(response.statusCode())
                .withFailMessage(() -> "token request failed: " + response.body())
                .isEqualTo(200);
        return JSON.readTree(response.body());
    }

    private HttpResponse<String> tokenRequest(String tokenEndpoint, String form) throws Exception {
        return httpExec(HttpRequest.newBuilder(URI.create(tokenEndpoint))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString(form))
                .build());
    }
}

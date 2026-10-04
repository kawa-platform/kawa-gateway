package io.jonasg.kawa.http;

import io.jonasg.kawa.config.AdminAuthConfig;
import io.jonasg.kawa.config.AdminConfig;
import io.jonasg.kawa.config.CorsConfig;
import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.GovernanceConfig;
import io.jonasg.kawa.core.cluster.BrokerNode;
import io.jonasg.kawa.core.cluster.MetadataCache;
import io.jonasg.kawa.core.cluster.MetadataSnapshot;
import io.jonasg.kawa.core.cluster.PartitionMetadata;
import io.jonasg.kawa.core.cluster.TopicMetadata;
import io.jonasg.kawa.governance.GovernancePolicy;
import io.jonasg.kawa.http.auth.AuthenticationProviderRegistry;
import io.jonasg.kawa.http.auth.builtin.BuiltInAuthenticationPlugin;
import io.jonasg.kawa.plugin.KawaPlugin;
import io.jonasg.kawa.test.TestSigningKeys;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import tools.jackson.databind.json.JsonMapper;

import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/// Shared setup for the admin HTTP slice tests: boots a real [AdminHttpServer] on an ephemeral
/// port wired to in-memory fakes, and sends real HTTP requests through the full Netty pipeline.
/// The slice tests assert the JSON wire format the admin UI consumes, so they exercise the
/// server bootstrap, routing, serialization and CORS handling - not just the plain handlers.
abstract class AdminHttpSliceTestBase {

    protected VirtualTopicManager virtualTopics = new VirtualTopicManager(Map.of());
    protected MetadataCache cache = new MetadataCache();
    protected FakeGatewayConfigRepository repository = new FakeGatewayConfigRepository(GatewayConfig.empty());
    protected GovernancePolicy governance = new GovernancePolicy(new GovernanceConfig(null));
    protected FakeTopicAdmin topicAdmin = new FakeTopicAdmin();
    protected CorsConfig cors;
    protected AdminAuthConfig auth = AdminAuthConfig.none();
    protected AuthenticationProviderRegistry authenticationProviders = registryOf(new BuiltInAuthenticationPlugin());

    private AdminHttpServer server;
    private final HttpClient client = HttpClient.newHttpClient();

    protected AdminHttpServer server() {
        return server;
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop();
        }
    }

    /// Boots the server with the current fakes. Call after configuring the fields.
    protected void startServer() throws InterruptedException {
        server = new AdminHttpServer(
                new AdminConfig(true, "127.0.0.1", 0, cors, auth),
                virtualTopics, cache, repository, governance, topicAdmin, authenticationProviders);
        server.start();
    }

    /// Sends a request to the running server and returns the response. A `null` body sends a
    /// request without a body.
    protected HttpResponse<String> send(String method, String path, String body) throws Exception {
        return send(method, path, body, Map.of());
    }

    protected HttpResponse<String> send(String method, String path) throws Exception {
        return send(method, path, null, Map.of());
    }

    /// Sends a request with extra headers to the running server and returns the response.
    protected HttpResponse<String> send(
            String method,
            String path,
            @Nullable String body,
            Map<String, String> headers
    ) throws Exception {
        HttpRequest.Builder builder = HttpRequest.newBuilder(
                URI.create("http://127.0.0.1:" + server.boundPort() + path));
        headers.forEach(builder::header);
        if (body == null) {
            builder.method(method, HttpRequest.BodyPublishers.noBody());
        } else {
            builder.method(method, HttpRequest.BodyPublishers.ofString(body));
        }
        return client.send(builder.build(), HttpResponse.BodyHandlers.ofString());
    }

    /// Switches the server to be started to `admin.auth.type: oauth`, with the bootstrap user
    /// `admin` / `s3cret` and the [TestSigningKeys#SEC1_P256] signing key.
    protected static final String LOGIN_URL = "http://localhost:5173/admin/login";
    protected static final String REDIRECT_URI = "http://localhost:5173/admin/auth/callback";

    protected void enableOAuth() {
        auth = new AdminAuthConfig("oauth", new AdminAuthConfig.BootstrapUser("admin", "s3cret"), Map.of(
                "issuer", "kawa-test", "signingKey", TestSigningKeys.SEC1_P256,
                "loginUrl", LOGIN_URL, "redirectUris", REDIRECT_URI));
    }

    /// A registry holding the providers the given plugins register, as the gateway's plugin loader
    /// would build it.
    protected static AuthenticationProviderRegistry registryOf(KawaPlugin... plugins) {
        var registry = new AuthenticationProviderRegistry();
        for (KawaPlugin plugin : plugins) {
            plugin.register(registry::register);
        }
        return registry;
    }

    /// `GET /oauth/authorize` for kawa studio with a PKCE challenge for `verifier`.
    protected HttpResponse<String> authorize(String verifier) throws Exception {
        return send("GET", "/oauth/authorize?response_type=code&client_id=kawa-studio&state=xyz"
                + "&redirect_uri=" + encode(REDIRECT_URI)
                + "&code_challenge=" + codeChallenge(verifier) + "&code_challenge_method=S256");
    }

    /// `POST /oauth/login` as the login UI does it.
    protected HttpResponse<String> loginWithChallenge(String challenge, String username, String password) throws Exception {
        return send("POST", "/oauth/login", JsonMapper.builder().build().writeValueAsString(Map.of(
                        "login_challenge", challenge, "username", username, "password", password)),
                Map.of("Content-Type", "application/json"));
    }

    /// Runs the whole authorization code flow with PKCE, as kawa studio and the browser do, and returns
    /// the token endpoint's response.
    protected HttpResponse<String> signIn(String username, String password) throws Exception {
        String verifier = "verifier-" + UUID.randomUUID() + "-" + UUID.randomUUID();
        String challenge = queryParam(authorize(verifier).headers().firstValue("Location").orElseThrow(), "login_challenge");
        var login = loginWithChallenge(challenge, username, password);
        if (login.statusCode() != 200) {
            return login;
        }
        String redirectTo = JsonMapper.builder().build().readTree(login.body()).get("redirect_to").asString();
        return send("POST", "/oauth/token", "grant_type=authorization_code&client_id=kawa-studio"
                        + "&code=" + encode(queryParam(redirectTo, "code"))
                        + "&redirect_uri=" + encode(REDIRECT_URI) + "&code_verifier=" + verifier,
                Map.of("Content-Type", "application/x-www-form-urlencoded"));
    }

    /// Signs in and returns the access token.
    protected String login(String username, String password) throws Exception {
        var response = signIn(username, password);
        if (response.statusCode() != 200) {
            throw new AssertionError("login as '" + username + "' failed: " + response.body());
        }
        return JsonMapper.builder().build().readTree(response.body()).get("access_token").asString();
    }

    protected static String queryParam(String url, String name) {
        return Arrays.stream(URI.create(url).getRawQuery().split("&"))
                .map(pair -> pair.split("=", 2))
                .filter(pair -> pair[0].equals(name))
                .map(pair -> URLDecoder.decode(pair[1], StandardCharsets.UTF_8))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no " + name + " in " + url));
    }

    private static String codeChallenge(String verifier) throws Exception {
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(verifier.getBytes(StandardCharsets.US_ASCII));
        return Base64.getUrlEncoder().withoutPadding().encodeToString(digest);
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    /// The `Authorization` header carrying `token`.
    protected static Map<String, String> bearer(String token) {
        return Map.of("Authorization", "Bearer " + token);
    }

    /// A metadata cache holding the given topics, mirroring a broker snapshot.
    protected static MetadataCache cacheWith(TopicMetadata... topics) {
        var cache = new MetadataCache();
        Map<String, TopicMetadata> topicMap = new HashMap<>();
        for (TopicMetadata topic : topics) {
            topicMap.put(topic.name(), topic);
        }
        cache.update(MetadataSnapshot.of(
                topicMap,
                Map.of(1, BrokerNode.of(1, "localhost", 9092, null)),
                "test-cluster"));
        return cache;
    }

    /// A topic with `partitions` partitions, each replicated `replicas` times.
    protected static TopicMetadata topic(String name, int partitions, int replicas) {
        var partitionList = new ArrayList<PartitionMetadata>();
        for (int i = 0; i < partitions; i++) {
            partitionList.add(PartitionMetadata.of(
                    i, 1, Collections.nCopies(replicas, 1), List.of(1), List.of()));
        }
        return TopicMetadata.of(name, partitionList);
    }

}

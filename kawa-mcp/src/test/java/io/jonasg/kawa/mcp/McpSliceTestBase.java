package io.jonasg.kawa.mcp;

import io.jonasg.kawa.config.AdminAuthConfig;
import io.jonasg.kawa.config.AdminConfig;
import io.jonasg.kawa.config.DynamicConfig;
import io.jonasg.kawa.config.GovernanceConfig;
import io.jonasg.kawa.config.ModuleConfig;
import io.jonasg.kawa.config.ModuleId;
import io.jonasg.kawa.core.cluster.BrokerNode;
import io.jonasg.kawa.core.cluster.MetadataCache;
import io.jonasg.kawa.core.cluster.MetadataSnapshot;
import io.jonasg.kawa.core.cluster.PartitionMetadata;
import io.jonasg.kawa.core.cluster.TopicMetadata;
import io.jonasg.kawa.governance.GovernancePolicy;
import io.jonasg.kawa.http.AdminHttpServer;
import io.jonasg.kawa.http.AdminServices;
import io.jonasg.kawa.http.authentication.AuthenticationProviderRegistry;
import io.jonasg.kawa.http.authentication.builtin.BuiltInAuthenticationPlugin;
import io.jonasg.kawa.http.topics.TopicService;
import io.jonasg.kawa.test.FakeBrokerAdmin;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import tools.jackson.databind.json.JsonMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/// Shared setup for the MCP slice tests: boots a real [AdminHttpServer] with the [KawaMcpServer] on an
/// ephemeral port, wired to in-memory state, and sends real JSON-RPC requests to `POST /mcp` through
/// the full Netty pipeline. Modelled on kawa-http-admin's `AdminHttpSliceTestBase`, which kawa-mcp
/// cannot share: it lives in that module's test sources.
public abstract class McpSliceTestBase {

    /// `admin.publicUrl`, which the `mcp` module needs to be switched on.
    protected static final String PUBLIC_URL = "https://kawa.example.com";

    protected VirtualTopicManager virtualTopics = new VirtualTopicManager(Map.of());
    protected MetadataCache cache = new MetadataCache();
    /// The stored config the server starts with: the `mcp` module switched on, as it is off by default.
    protected DynamicConfig config = DynamicConfig.empty().upsertModule(new ModuleConfig(ModuleId.MCP, true));
    /// Replaces the real topic service when set, e.g. with a mock that fails.
    protected @Nullable TopicService topicService;
    /// The broker the topic service talks to: topics in here exist on the broker, as `states`.
    protected FakeBrokerAdmin brokerAdmin = new FakeBrokerAdmin();

    private final HttpClient client = HttpClient.newHttpClient();
    private @Nullable AdminServices services;
    private @Nullable AdminHttpServer server;
    private @Nullable KawaMcpServer mcp;

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop();
        }
        if (mcp != null) {
            mcp.close();
        }
        if (services != null) {
            services.close();
        }
    }

    /// Boots the server with the current state. Call after configuring the fields.
    protected void startServer() throws InterruptedException {
        var adminServices = AdminServices.create(
                virtualTopics,
                cache,
                () -> CompletableFuture.completedFuture(null),
                Duration.ofSeconds(5),
                new InMemoryConfigRepository(config),
                new GovernancePolicy(new GovernanceConfig(null)),
                brokerAdmin,
                null,
                PUBLIC_URL,
                _ -> null
        );
        services = topicService == null ? adminServices : new AdminServices(
                topicService,
                adminServices.consumerGroups(),
                adminServices.roles(),
                adminServices.groups(),
                adminServices.clients(),
                adminServices.governanceRules(),
                adminServices.adminUsers(),
                adminServices.adminUserAuthenticator(),
                adminServices.modules(),
                adminServices.oauthClients(),
                adminServices.oauthClientDirectory()
        );
        var authenticationProviders = new AuthenticationProviderRegistry();
        new BuiltInAuthenticationPlugin().register(authenticationProviders::register);
        server = new AdminHttpServer(new AdminConfig(true, "127.0.0.1", 0, null, AdminAuthConfig.none(), PUBLIC_URL), services,
                authenticationProviders);
        mcp = new KawaMcpServer(server.router(), services, null);
        server.start();
    }

    /// Sends a request to the admin API, e.g. `PUT /modules/mcp`. A `null` body sends none.
    protected HttpResponse<String> send(String method, String path, @Nullable String body) throws Exception {
        var request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.boundPort() + path))
                .header("Content-Type", "application/json")
                .method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body))
                .build();
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    /// Sends the JSON-RPC request `method` with `params` (a JSON object) to `POST /mcp`.
    protected HttpResponse<String> rpc(String method, String params) throws Exception {
        String body = "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"" + method + "\",\"params\":" + params + "}";
        var request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.boundPort() + KawaMcpServer.PATH))
                .header("Content-Type", "application/json")
                .header("Accept", "application/json, text/event-stream")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    /// Calls the tool `name` without arguments.
    protected HttpResponse<String> callTool(String name) throws Exception {
        return rpc("tools/call", "{\"name\":\"" + name + "\",\"arguments\":{}}");
    }

    /// Calls the tool `name` with arguments.
    protected HttpResponse<String> callToolWithArgs(String name, String args) throws Exception {
        return rpc("tools/call", "{\"name\":\"" + name + "\",\"arguments\":%s}".formatted(args));
    }

    /// A metadata cache holding the given topics, mirroring a broker snapshot.
    protected static MetadataCache cacheWith(TopicMetadata... topics) {
        var cache = new MetadataCache();
        Map<String, TopicMetadata> topicMap = new HashMap<>();
        for (TopicMetadata topic : topics) {
            topicMap.put(topic.name(), topic);
        }
        cache.update(MetadataSnapshot.of(topicMap, Map.of(1, BrokerNode.of(1, "localhost", 9092, null)), "test-cluster"));
        return cache;
    }

    /// A topic with `partitions` partitions, each replicated `replicas` times.
    protected static TopicMetadata topic(String name, int partitions, int replicas) {
        var partitionList = new ArrayList<PartitionMetadata>();
        for (int i = 0; i < partitions; i++) {
            partitionList.add(PartitionMetadata.of(i, 1, Collections.nCopies(replicas, 1), List.of(1), List.of()));
        }
        return TopicMetadata.of(name, partitionList);
    }

    public String structuredContent(String body) {
        var mapper = new JsonMapper();
        return mapper.writeValueAsString(mapper.readTree(body).get("result").get("structuredContent"));
    }
}

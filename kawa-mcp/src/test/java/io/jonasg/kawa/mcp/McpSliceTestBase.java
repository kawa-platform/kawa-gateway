package io.jonasg.kawa.mcp;

import io.jonasg.kawa.config.AdminAuthConfig;
import io.jonasg.kawa.config.AdminConfig;
import io.jonasg.kawa.config.DynamicConfig;
import io.jonasg.kawa.config.DynamicConfigRepository;
import io.jonasg.kawa.config.GovernanceConfig;
import io.jonasg.kawa.core.cluster.BrokerNode;
import io.jonasg.kawa.core.cluster.MetadataCache;
import io.jonasg.kawa.core.cluster.MetadataSnapshot;
import io.jonasg.kawa.core.cluster.PartitionMetadata;
import io.jonasg.kawa.core.cluster.TopicMetadata;
import io.jonasg.kawa.core.module.ModuleRegistry;
import io.jonasg.kawa.governance.GovernancePolicy;
import io.jonasg.kawa.http.AdminHttpServer;
import io.jonasg.kawa.http.AdminServices;
import io.jonasg.kawa.http.authentication.AuthenticationProviderRegistry;
import io.jonasg.kawa.http.authentication.builtin.BuiltInAuthenticationPlugin;
import io.jonasg.kawa.http.topics.TopicService;
import io.jonasg.kawa.test.FakeTopicAdmin;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import tools.jackson.databind.json.JsonMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

/// Shared setup for the MCP slice tests: boots a real [AdminHttpServer] with the [KawaMcpServer] on an
/// ephemeral port, wired to in-memory state, and sends real JSON-RPC requests to `POST /mcp` through
/// the full Netty pipeline. Modelled on kawa-http-admin's `AdminHttpSliceTestBase`, which kawa-mcp
/// cannot share: it lives in that module's test sources.
public abstract class McpSliceTestBase {

    protected VirtualTopicManager virtualTopics = new VirtualTopicManager(Map.of());
    protected MetadataCache cache = new MetadataCache();
    /// The gateway's modules, reloaded from the config on every write as the config consumer would.
    protected ModuleRegistry modules = new ModuleRegistry();
    /// Replaces the real topic service when set, e.g. with a mock that fails.
    protected @Nullable TopicService topicService;
    /// The broker the topic service talks to: topics in here exist on the broker, as `states`.
    protected FakeTopicAdmin topicAdmin = new FakeTopicAdmin();

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
        var real = AdminServices.create(virtualTopics, cache, new InMemoryConfigRepository(modules),
                new GovernancePolicy(new GovernanceConfig(null)), topicAdmin, null, _ -> null);
        services = topicService == null ? real : new AdminServices(topicService, real.roles(), real.groups(),
                real.clients(), real.governanceRules(), real.adminUsers(), real.adminUserAuthenticator(), real.modules(),
                real.oauthClients(), real.oauthClientDirectory());
        var authenticationProviders = new AuthenticationProviderRegistry();
        new BuiltInAuthenticationPlugin().register(authenticationProviders::register);
        server = new AdminHttpServer(new AdminConfig(true, "127.0.0.1", 0, null, AdminAuthConfig.none()), services,
                authenticationProviders);
        mcp = new KawaMcpServer(server.router(), services, modules, null);
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

    /// Holds the config in memory; a write applies at once and reloads the modules from it.
    private static final class InMemoryConfigRepository implements DynamicConfigRepository {

        private final ModuleRegistry modules;
        private DynamicConfig current = DynamicConfig.empty();

        private InMemoryConfigRepository(ModuleRegistry modules) {
            this.modules = modules;
        }

        @Override
        public DynamicConfig getActiveConfig() {
            return current;
        }

        @Override
        public synchronized void update(UnaryOperator<DynamicConfig> mutation) {
            current = mutation.apply(current);
            modules.reload(current.modules());
        }

        @Override
        public void updateAndWaitUntilApplied(UnaryOperator<DynamicConfig> mutation) {
            update(mutation);
        }

        @Override
        public void close() {
        }
    }
}

package io.jonasg.kawa.server;

import io.jonasg.kawa.config.AdminConfig;
import io.jonasg.kawa.core.cluster.MetadataCache;
import io.jonasg.kawa.http.AdminHttpServer;
import io.jonasg.kawa.http.KafkaTopicAdmin;
import io.jonasg.kawa.http.auth.AuthenticationProviderRegistry;
import io.jonasg.kawa.mcp.KawaMcpServer;

import java.util.Properties;

/// The admin HTTP surface: the [KafkaTopicAdmin] that creates/deletes physical topics on the
/// broker and the [AdminHttpServer] that exposes the gateway's state and dynamic config to a
/// UI, plus the [KawaMcpServer] served at `/mcp` on the same port. Config writes go through the
/// [DynamicGatewayState]'s config manager to the config topic.
public final class AdminSurface implements AutoCloseable {

    private final AdminHttpServer server;
    private final KawaMcpServer mcp;

    public AdminSurface(AdminConfig config, DynamicGatewayState state, MetadataCache cache,
                        String bootstrapServers, Properties brokerSecurity,
                        AuthenticationProviderRegistry authenticationProviders) {
        var topicAdmin = new KafkaTopicAdmin(bootstrapServers, brokerSecurity);
        server = new AdminHttpServer(config, state.virtualTopics(), cache, state.configManager(),
                state.governance(), topicAdmin, authenticationProviders);
        mcp = new KawaMcpServer(server.router(), config.cors());
    }

    public void start() throws InterruptedException {
        server.start();
    }

    public int boundPort() {
        return server.boundPort();
    }

    @Override
    public void close() {
        mcp.close();
        server.stop(); // AdminHttpServer.stop() also closes the TopicAdmin
    }
}

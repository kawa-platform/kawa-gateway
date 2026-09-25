package io.jonasg.kawa.server.broker;

import io.jonasg.kawa.config.BrokerAuthConfig;
import io.jonasg.kawa.core.InterceptorPipeline;
import io.jonasg.kawa.core.cluster.BrokerNode;
import io.jonasg.kawa.core.cluster.MetadataCache;
import io.jonasg.kawa.core.metrics.GatewayMetrics;
import io.jonasg.kawa.protocol.kafka.KafkaBodyCodec;
import io.jonasg.kawa.server.netty.ClientSession;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.ssl.SslContext;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/// Lazily creates and reuses one [BrokerClient] per physical broker. Unknown broker
/// ids (or the UNROUTED route) fall back to a connection to the bootstrap node.
public final class BrokerClientPool {

    private final Map<Integer, BrokerClient> clients = new ConcurrentHashMap<>();
    private final EventLoopGroup group;
    private final KafkaBodyCodec codec;
    private final InterceptorPipeline pipeline;
    private final GatewayMetrics metrics;
    private final MetadataCache cache;
    private final BrokerAuthConfig brokerAuthConfig;
    private final SslContext sslContext;
    private final BrokerClient bootstrap;

    public BrokerClientPool(EventLoopGroup group, KafkaBodyCodec codec, InterceptorPipeline pipeline,
                            GatewayMetrics metrics, MetadataCache cache, String bootstrapHost, int bootstrapPort) {
        this(group, codec, pipeline, metrics, cache, bootstrapHost, bootstrapPort, null);
    }

    public BrokerClientPool(EventLoopGroup group, KafkaBodyCodec codec, InterceptorPipeline pipeline,
                            GatewayMetrics metrics, MetadataCache cache, String bootstrapHost, int bootstrapPort,
                            BrokerAuthConfig brokerAuthConfig) {
        this(group, codec, pipeline, metrics, cache, bootstrapHost, bootstrapPort, brokerAuthConfig, null);
    }

    public BrokerClientPool(EventLoopGroup group, KafkaBodyCodec codec, InterceptorPipeline pipeline,
                            GatewayMetrics metrics, MetadataCache cache, String bootstrapHost, int bootstrapPort,
                            BrokerAuthConfig brokerAuthConfig, SslContext sslContext) {
        this.group = group;
        this.codec = codec;
        this.pipeline = pipeline;
        this.metrics = metrics;
        this.cache = cache;
        this.brokerAuthConfig = brokerAuthConfig;
        this.sslContext = sslContext;
        this.bootstrap = new BrokerClient(-1, bootstrapHost, bootstrapPort, group, codec, pipeline, metrics,
                brokerAuthConfig, sslContext);
    }

    public BrokerClient forBroker(int brokerId) {
        if (brokerId < 0) {
            return bootstrap;
        }
        return clients.computeIfAbsent(brokerId, id -> {
            BrokerNode node = cache.broker(id);
            if (node == null) {
                return bootstrap;
            }
            return new BrokerClient(id, node.host(), node.port(), group, codec, pipeline, metrics,
                    brokerAuthConfig, sslContext);
        });
    }

    public void prune(Collection<BrokerNode> nodes) {
        Map<Integer, BrokerNode> current = new java.util.HashMap<>();
        for (BrokerNode node : nodes) {
            current.put(node.id(), node);
        }
        clients.keySet().removeIf(id -> !current.containsKey(id));
    }

    public void closeSession(ClientSession session) {
        bootstrap.closeSession(session);
        clients.values().forEach(client -> client.closeSession(session));
    }

    public void closeAll() {
        bootstrap.close();
        clients.values().forEach(BrokerClient::close);
        clients.clear();
    }
}

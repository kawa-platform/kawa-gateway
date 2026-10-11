package io.jonasg.kawa.server.broker;

import io.jonasg.kawa.config.BrokerAuthConfig;
import io.jonasg.kawa.core.InterceptorPipeline;
import io.jonasg.kawa.core.cluster.BrokerNode;
import io.jonasg.kawa.core.cluster.MetadataCache;
import io.jonasg.kawa.core.metrics.GatewayMetrics;
import io.jonasg.kawa.protocol.kafka.KafkaBodyCodec;
import io.jonasg.kawa.server.netty.ClientSession;
import io.netty.channel.EventLoopGroup;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/// Lazily creates one [BrokerClient] per client connection and physical broker, the connections
/// that client would have if it talked to the cluster directly. Unknown broker ids (or the
/// UNROUTED route) fall back to a connection to the bootstrap node.
///
/// A broker processes one request per connection at a time, so connections are not shared between
/// clients: a request the broker holds (a JoinGroup until the group has rejoined, a Fetch until
/// data arrives) would otherwise block every other client's requests to that broker behind it.
/// The cost is a connection per client and broker, and a connect (and broker SASL handshake) on a
/// client's first request to each broker.
public final class BrokerClientPool {

    private static final int BOOTSTRAP = -1;

    private final Map<ClientSession, Map<Integer, BrokerClient>> clients = new ConcurrentHashMap<>();
    private final EventLoopGroup group;
    private final KafkaBodyCodec codec;
    private final InterceptorPipeline pipeline;
    private final GatewayMetrics metrics;
    private final MetadataCache cache;
    private final BrokerAuthConfig brokerAuthConfig;
    private final String bootstrapHost;
    private final int bootstrapPort;

    public BrokerClientPool(EventLoopGroup group, KafkaBodyCodec codec, InterceptorPipeline pipeline,
                            GatewayMetrics metrics, MetadataCache cache, String bootstrapHost, int bootstrapPort) {
        this(group, codec, pipeline, metrics, cache, bootstrapHost, bootstrapPort, null);
    }

    public BrokerClientPool(EventLoopGroup group, KafkaBodyCodec codec, InterceptorPipeline pipeline,
                            GatewayMetrics metrics, MetadataCache cache, String bootstrapHost, int bootstrapPort,
                            BrokerAuthConfig brokerAuthConfig) {
        this.group = group;
        this.codec = codec;
        this.pipeline = pipeline;
        this.metrics = metrics;
        this.cache = cache;
        this.brokerAuthConfig = brokerAuthConfig;
        this.bootstrapHost = bootstrapHost;
        this.bootstrapPort = bootstrapPort;
    }

    /// The connection `session` uses for `brokerId`, or for the bootstrap node when the broker is
    /// not known.
    public BrokerClient forBroker(ClientSession session, int brokerId) {
        BrokerNode node = brokerId < 0 ? null : cache.broker(brokerId);
        Map<Integer, BrokerClient> sessionClients = clients.computeIfAbsent(session, _ -> new ConcurrentHashMap<>());
        if (node == null) {
            return sessionClients.computeIfAbsent(BOOTSTRAP, _ -> newClient(BOOTSTRAP, bootstrapHost, bootstrapPort));
        }
        return sessionClients.computeIfAbsent(brokerId, id -> newClient(id, node.host(), node.port()));
    }

    private BrokerClient newClient(int brokerId, String host, int port) {
        return new BrokerClient(brokerId, host, port, group, codec, pipeline, metrics, brokerAuthConfig);
    }

    /// Closes the connections of brokers that left the cluster, in every session.
    public void prune(Collection<BrokerNode> nodes) {
        Set<Integer> current = nodes.stream().map(BrokerNode::id).collect(Collectors.toSet());
        clients.values().forEach(sessionClients -> sessionClients.entrySet().removeIf(entry -> {
            if (entry.getKey() == BOOTSTRAP || current.contains(entry.getKey())) {
                return false;
            }
            entry.getValue().close();
            return true;
        }));
    }

    /// Closes every broker connection of a client that disconnected.
    public void closeSession(ClientSession session) {
        Map<Integer, BrokerClient> sessionClients = clients.remove(session);
        if (sessionClients != null) {
            sessionClients.values().forEach(BrokerClient::close);
        }
    }

    public void closeAll() {
        clients.values().forEach(sessionClients -> sessionClients.values().forEach(BrokerClient::close));
        clients.clear();
    }
}

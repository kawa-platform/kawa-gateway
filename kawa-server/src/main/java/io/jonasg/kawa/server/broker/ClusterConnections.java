package io.jonasg.kawa.server.broker;

import io.jonasg.kawa.core.InterceptorPipeline;
import io.jonasg.kawa.core.cluster.MetadataCache;
import io.jonasg.kawa.core.metrics.GatewayMetrics;
import io.jonasg.kawa.protocol.kafka.KafkaBodyCodec;
import io.jonasg.kawa.server.LeaderRouter;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import java.net.InetSocketAddress;
import java.util.List;

/// The gateway's own connections to the cluster: the broker client pool, the metadata client
/// that refreshes the [MetadataCache] and captures the broker's ApiVersions ranges, and the
/// [LeaderRouter] that routes requests to the leader of a topic's first partition.
public final class ClusterConnections implements AutoCloseable {

    private final EventLoopGroup brokerGroup = new NioEventLoopGroup();
    private final BrokerClientPool pool;
    private final MetadataClient metadataClient;
    private final LeaderRouter router;

    /// `bootstrap` lists the cluster's bootstrap servers in configured order; the first one is the
    /// fallback for requests to a broker the cache does not know.
    public ClusterConnections(KafkaBodyCodec codec, InterceptorPipeline pipeline,
                              GatewayMetrics metrics, MetadataCache cache,
                              List<InetSocketAddress> bootstrap, BrokerConnectionSettings settings) {
        var first = bootstrap.getFirst();
        pool = new BrokerClientPool(brokerGroup, codec, pipeline, metrics, cache,
                first.getHostString(), first.getPort(), settings);
        metadataClient = new MetadataClient(bootstrap, brokerGroup, codec, cache, pool, metrics, settings);
        router = new LeaderRouter(cache);
    }

    /// Starts the metadata client, which blocks until the initial metadata fetch completes.
    public void start() throws Exception {
        metadataClient.start();
    }

    public BrokerClientPool pool() {
        return pool;
    }

    public MetadataClient metadataClient() {
        return metadataClient;
    }

    public LeaderRouter router() {
        return router;
    }

    @Override
    public void close() {
        metadataClient.stop();
        pool.closeAll();
        brokerGroup.shutdownGracefully();
    }
}

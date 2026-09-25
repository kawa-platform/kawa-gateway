package io.jonasg.kawa.server.broker;

import io.jonasg.kawa.config.BrokerAuthConfig;
import io.jonasg.kawa.core.InterceptorPipeline;
import io.jonasg.kawa.core.cluster.MetadataCache;
import io.jonasg.kawa.core.metrics.GatewayMetrics;
import io.jonasg.kawa.protocol.kafka.KafkaBodyCodec;
import io.jonasg.kawa.server.LeaderRouter;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

/// The gateway's own connections to the cluster: the broker client pool, the metadata client
/// that refreshes the [MetadataCache] and captures the broker's ApiVersions ranges, and the
/// [LeaderRouter] that routes requests to the leader of a topic's first partition.
public final class ClusterConnections implements AutoCloseable {

    private final EventLoopGroup brokerGroup = new NioEventLoopGroup();
    private final BrokerClientPool pool;
    private final MetadataClient metadataClient;
    private final LeaderRouter router;
    private final SslContext sslContext;

    public ClusterConnections(KafkaBodyCodec codec, InterceptorPipeline pipeline,
                              GatewayMetrics metrics, MetadataCache cache,
                              String host, int port, BrokerAuthConfig brokerAuth) {
        sslContext = isIam(brokerAuth) ? buildSslContext() : null;
        pool = new BrokerClientPool(
                brokerGroup, codec, pipeline, metrics, cache, host, port, brokerAuth, sslContext);
        metadataClient = new MetadataClient(
                host, port, brokerGroup, codec, cache, pool, metrics, brokerAuth, sslContext);
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

    boolean tlsEnabled() {
        return sslContext != null;
    }

    @Override
    public void close() {
        metadataClient.stop();
        pool.closeAll();
        brokerGroup.shutdownGracefully();
    }

    private static boolean isIam(BrokerAuthConfig brokerAuth) {
        return brokerAuth != null && "AWS_MSK_IAM".equals(brokerAuth.mechanism());
    }

    private static SslContext buildSslContext() {
        try {
            return SslContextBuilder.forClient().build();
        } catch (Exception e) {
            throw new IllegalStateException("Unable to create TLS context for broker connection", e);
        }
    }
}

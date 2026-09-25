package io.jonasg.kawa.server.broker;

import io.jonasg.kawa.config.BrokerAuthConfig;
import io.jonasg.kawa.core.cluster.BrokerNode;
import io.jonasg.kawa.core.cluster.MetadataCache;
import io.jonasg.kawa.core.cluster.MetadataSnapshot;
import io.jonasg.kawa.core.cluster.PartitionMetadata;
import io.jonasg.kawa.core.cluster.TopicMetadata;
import io.jonasg.kawa.core.metrics.GatewayMetrics;
import io.jonasg.kawa.protocol.kafka.KafkaBodyCodec;
import io.jonasg.kawa.protocol.kafka.KafkaFrameDecoder;
import io.jonasg.kawa.protocol.kafka.KafkaFrameEncoder;
import io.jonasg.kawa.protocol.kafka.KafkaHeader;
import io.jonasg.kawa.protocol.kafka.RequestHeaderCodec;
import io.jonasg.kawa.protocol.kafka.ResponseHeaderCodec;
import io.jonasg.kawa.protocol.kafka.VersionRange;
import io.jonasg.kawa.server.auth.BrokerSaslAuthenticator;
import io.jonasg.kawa.server.auth.BrokerSaslMechanism;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/// The gateway's own connection to the cluster. Refreshes the [MetadataCache] used for
/// routing and captures the broker's ApiVersions ranges so the gateway can advertise a safe
/// intersection to clients.
public final class MetadataClient {

    private static final Logger log = LoggerFactory.getLogger(MetadataClient.class);
    private static final short API_VERSIONS = 18;
    private static final short METADATA = 3;
    private static final long REFRESH_SECONDS = 10;
    private static final long INITIAL_METADATA_TIMEOUT_SECONDS = 30;

    private final String host;
    private final int port;
    private final EventLoopGroup group;
    private final KafkaBodyCodec codec;
    private final MetadataCache cache;
    private final BrokerClientPool pool;
    private final GatewayMetrics metrics;
    private final BrokerAuthConfig brokerAuthConfig;
    private final SslContext sslContext;
    private final RequestHeaderCodec requestHeaderCodec = new RequestHeaderCodec();
    private final ResponseHeaderCodec responseHeaderCodec = new ResponseHeaderCodec();
    private final Map<Integer, Consumer<ByteBuf>> pending = new ConcurrentHashMap<>();
    private final AtomicInteger correlationIds = new AtomicInteger();
    private final CountDownLatch initialMetadataLatch = new CountDownLatch(1);

    private volatile Channel channel;
    private volatile boolean running;
    private volatile Map<Short, VersionRange> brokerRanges = Map.of();

    public MetadataClient(String host, int port, EventLoopGroup group, KafkaBodyCodec codec,
                          MetadataCache cache, BrokerClientPool pool, GatewayMetrics metrics) {
        this(host, port, group, codec, cache, pool, metrics, null);
    }

    public MetadataClient(String host, int port, EventLoopGroup group, KafkaBodyCodec codec,
                          MetadataCache cache, BrokerClientPool pool, GatewayMetrics metrics,
                          BrokerAuthConfig brokerAuthConfig) {
        this(host, port, group, codec, cache, pool, metrics, brokerAuthConfig, null);
    }

    public MetadataClient(String host, int port, EventLoopGroup group, KafkaBodyCodec codec,
                          MetadataCache cache, BrokerClientPool pool, GatewayMetrics metrics,
                          BrokerAuthConfig brokerAuthConfig, SslContext sslContext) {
        this.host = host;
        this.port = port;
        this.group = group;
        this.codec = codec;
        this.cache = cache;
        this.pool = pool;
        this.metrics = metrics;
        this.brokerAuthConfig = brokerAuthConfig;
        this.sslContext = sslContext;
    }

    public void start() throws Exception {
        var bootstrap = new Bootstrap();
        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        if (sslContext != null) {
                            ch.pipeline().addLast("ssl", sslContext.newHandler(ch.alloc(), host, port));
                        }
                        ch.pipeline().addLast("frameDecoder", new KafkaFrameDecoder());
                        ch.pipeline().addLast("frameEncoder", new KafkaFrameEncoder());
                        ch.pipeline().addLast("handler", new MetadataResponseHandler(MetadataClient.this));
                    }
                });
        channel = connectWithRetry(bootstrap);
        SslHandler sslHandler = channel.pipeline().get(SslHandler.class);
        if (sslHandler != null) {
            sslHandler.handshakeFuture().sync();
        }
        if (brokerAuthConfig != null) {
            var authenticator = new BrokerSaslAuthenticator(
                    BrokerSaslMechanism.from(brokerAuthConfig), codec, host);
            if (!authenticator.authenticate(channel)) {
                channel.close();
                throw new IllegalStateException("SASL authentication to metadata broker failed");
            }
        }
        metrics.brokerConnectionOpened();
        log.info("Metadata client connected to {}:{} remote {} active {} autoRead {}",
                host, port, channel.remoteAddress(), channel.isActive(),
                channel.config().isAutoRead());
        channel.closeFuture().addListener(_ -> {
            metrics.brokerConnectionClosed();
            if (running) {
                log.warn("Metadata connection to {}:{} lost; will not refresh topology", host, port);
            }
        });
        running = true;
        refresh();

        if (!initialMetadataLatch.await(INITIAL_METADATA_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
            throw new IllegalStateException("Timed out waiting for initial metadata from " + host + ":" + port);
        }
    }

    /// Connects to the bootstrap broker, retrying within the initial-metadata timeout window.
    /// A broker that is still starting (listener not yet bound) refuses connections; the
    /// gateway should tolerate that instead of failing startup on the first refusal.
    Channel connectWithRetry(Bootstrap bootstrap) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(INITIAL_METADATA_TIMEOUT_SECONDS);
        while (true) {
            try {
                return bootstrap.connect(host, port).sync().channel();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw e;
            } catch (Exception e) {
                if (System.nanoTime() >= deadline) {
                    throw e;
                }
                log.warn("Metadata connect to {}:{} failed ({}); retrying", host, port, e.getMessage());
                Thread.sleep(500);
            }
        }
    }

    public Map<Short, VersionRange> brokerRanges() {
        return brokerRanges;
    }

    private void refresh() {
        Channel ch = channel;
        if (!running || ch == null || !ch.isActive()) {
            return;
        }
        sendApiVersions(ch);
        sendMetadata(ch);
        ch.eventLoop().schedule(this::refresh, REFRESH_SECONDS, java.util.concurrent.TimeUnit.SECONDS);
    }

    private void sendApiVersions(Channel ch) {
        var request = new ApiVersionsRequestData()
                .setClientSoftwareName("kawa-gateway")
                .setClientSoftwareVersion("0.1.0");
        int correlationId = correlationIds.incrementAndGet();
        pending.put(correlationId, this::onApiVersionsResponse);
        write(ch, API_VERSIONS, (short) 3, correlationId, request);
    }

    private void sendMetadata(Channel ch) {
        // A null `topics` array requests metadata for ALL topics; an empty array requests none.
        var request = new MetadataRequestData().setTopics(null);
        int correlationId = correlationIds.incrementAndGet();
        pending.put(correlationId, this::onMetadataResponse);
        write(ch, METADATA, (short) 8, correlationId, request);
    }

    private void write(
            Channel ch,
            short apiKey,
            short version,
            int correlationId,
            Object body
    ) {
        var header = KafkaHeader.of(apiKey, version, correlationId, "kawa-metadata-client");
        ByteBuf out = ch.alloc().buffer();
        requestHeaderCodec.encode(out, header);
        codec.encodeRequest(apiKey, version, body, out);
        if (log.isDebugEnabled()) {
            log.debug("Sending {} v{} (correlation {}) to broker: {}",
                    apiKey, version, correlationId, hex(out));
        }
        ch.writeAndFlush(out);
    }

    private static String hex(ByteBuf buf) {
        var sb = new StringBuilder();
        for (int i = 0; i < buf.writerIndex(); i++) {
            sb.append(String.format("%02x ", buf.getByte(i)));
        }
        return sb.toString();
    }

    void handleFrame(ByteBuf frame) {
        if (frame.readableBytes() < 4) {
            frame.release();
            return;
        }
        int correlationId = frame.getInt(frame.readerIndex());
        Consumer<ByteBuf> consumer = pending.remove(correlationId);
        if (consumer == null) {
            log.warn("Dropping metadata response for unknown correlation id {}", correlationId);
            frame.release();
            return;
        }
        try {
            consumer.accept(frame);
        } catch (Exception e) {
            log.error("Error handling metadata response correlation {}", correlationId, e);
            frame.release();
        }
    }

    private void onApiVersionsResponse(ByteBuf frame) {
        try {
            responseHeaderCodec.decodeCorrelationId(frame, ApiKeys.API_VERSIONS.responseHeaderVersion((short) 3));
            ApiVersionsResponseData data =
                    (ApiVersionsResponseData) codec.decodeResponse(API_VERSIONS, (short) 3, frame);
            Map<Short, VersionRange> ranges = new HashMap<>();
            for (ApiVersionsResponseData.ApiVersion apiVersion : data.apiKeys()) {
                ranges.put(apiVersion.apiKey(),
                        VersionRange.of(apiVersion.minVersion(), apiVersion.maxVersion()));
            }
            brokerRanges = Map.copyOf(ranges);
            log.info("Captured broker ApiVersions for {} api keys", ranges.size());
        } finally {
            frame.release();
        }
    }

    private void onMetadataResponse(ByteBuf frame) {
        try {
            responseHeaderCodec.decodeCorrelationId(frame, ApiKeys.METADATA.responseHeaderVersion((short) 8));
            MetadataResponseData data =
                    (MetadataResponseData) codec.decodeResponse(METADATA, (short) 8, frame);
            MetadataSnapshot snapshot = toSnapshot(data);
            cache.update(snapshot);
            pool.prune(snapshot.brokers().values());
            // No-op after the first response; the latch only gates startup.
            initialMetadataLatch.countDown();
            log.info("Refreshed topology: {} brokers, {} topics, cluster {}",
                    snapshot.brokers().size(), snapshot.topics().size(), snapshot.clusterId());
        } finally {
            frame.release();
        }
    }

    private static MetadataSnapshot toSnapshot(MetadataResponseData data) {
        Map<String, TopicMetadata> topics = new HashMap<>();
        for (MetadataResponseData.MetadataResponseTopic topic : data.topics()) {
            if (topic.errorCode() != 0) {
                continue;
            }
            List<PartitionMetadata> partitions = new ArrayList<>();
            for (MetadataResponseData.MetadataResponsePartition partition : topic.partitions()) {
                partitions.add(PartitionMetadata.of(
                        partition.partitionIndex(),
                        partition.leaderId(),
                        partition.replicaNodes(),
                        partition.isrNodes(),
                        partition.offlineReplicas()));
            }
            topics.put(topic.name(), TopicMetadata.of(topic.name(), partitions));
        }

        var brokers = data.brokers().stream()
                .collect(
                        Collectors.toMap(MetadataResponseData.MetadataResponseBroker::nodeId,
                                b -> BrokerNode.of(b.nodeId(), b.host(), b.port(), b.rack()))
                );
        return MetadataSnapshot.of(topics, brokers, data.clusterId());
    }

    public void stop() {
        running = false;
        Channel ch = channel;
        if (ch != null) {
            ch.close();
        }
        pending.clear();
    }
}

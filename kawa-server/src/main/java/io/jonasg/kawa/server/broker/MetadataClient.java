package io.jonasg.kawa.server.broker;

import io.jonasg.kawa.core.cluster.BrokerNode;
import io.jonasg.kawa.core.cluster.MetadataCache;
import io.jonasg.kawa.core.cluster.MetadataRefresher;
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
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/// The gateway's own connection to the cluster. Refreshes the [MetadataCache] used for
/// routing and captures the broker's ApiVersions ranges so the gateway can advertise a safe
/// intersection to clients. Besides the scheduled refresh, [#refresh()] fetches the metadata on
/// demand, so the admin API sees a topic it just created without waiting for the next schedule.
///
/// When the connection drops, it is re-established in the background: the bootstrap servers and
/// the brokers the cache last learned are tried in order, with backoff from 0.5 s up to 30 s, until
/// one answers. Until then [#refresh()] fails and the cache keeps its last known topology.
public final class MetadataClient implements MetadataRefresher {

    private static final Logger log = LoggerFactory.getLogger(MetadataClient.class);
    private static final short API_VERSIONS = 18;
    private static final short METADATA = 3;
    private static final long REFRESH_SECONDS = 10;
    private static final long INITIAL_METADATA_TIMEOUT_SECONDS = 30;
    private static final long INITIAL_BACKOFF_MILLIS = 500;
    private static final long MAX_BACKOFF_MILLIS = 30_000;
    private static final int CONNECT_TIMEOUT_MILLIS = 5_000;

    private final List<Endpoint> bootstrap;
    private final EventLoopGroup group;
    private final KafkaBodyCodec codec;
    private final MetadataCache cache;
    private final BrokerClientPool pool;
    private final GatewayMetrics metrics;
    private final BrokerConnectionSettings settings;
    private final RequestHeaderCodec requestHeaderCodec = new RequestHeaderCodec();
    private final ResponseHeaderCodec responseHeaderCodec = new ResponseHeaderCodec();
    private final AtomicInteger correlationIds = new AtomicInteger();
    private final CountDownLatch initialMetadataLatch = new CountDownLatch(1);
    private final ExecutorService reconnector = Executors.newSingleThreadExecutor(runnable -> {
        var thread = new Thread(runnable, "kawa-metadata-reconnect");
        thread.setDaemon(true);
        return thread;
    });

    private volatile @Nullable Connection connection;
    private volatile @Nullable Endpoint lastAnswered;
    private volatile boolean running;
    private volatile Map<Short, VersionRange> brokerRanges = Map.of();

    public MetadataClient(List<InetSocketAddress> bootstrap, EventLoopGroup group, KafkaBodyCodec codec,
                          MetadataCache cache, BrokerClientPool pool, GatewayMetrics metrics) {
        this(bootstrap, group, codec, cache, pool, metrics, BrokerConnectionSettings.PLAINTEXT);
    }

    public MetadataClient(List<InetSocketAddress> bootstrap, EventLoopGroup group, KafkaBodyCodec codec,
                          MetadataCache cache, BrokerClientPool pool, GatewayMetrics metrics,
                          BrokerConnectionSettings settings) {
        this.bootstrap = bootstrap.stream().map(Endpoint::of).toList();
        this.group = group;
        this.codec = codec;
        this.cache = cache;
        this.pool = pool;
        this.metrics = metrics;
        this.settings = settings;
    }

    public void start() throws Exception {
        running = true;
        onConnected(connectWithRetry());
        if (!initialMetadataLatch.await(INITIAL_METADATA_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
            throw new IllegalStateException("Timed out waiting for initial metadata from " + bootstrap);
        }
    }

    /// Connects to the first candidate that answers, retrying with backoff within the initial-metadata
    /// timeout window. A broker that is still starting (listener not yet bound) refuses connections;
    /// the gateway should tolerate that instead of failing startup on the first refusal.
    Connection connectWithRetry() throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(INITIAL_METADATA_TIMEOUT_SECONDS);
        long delay = INITIAL_BACKOFF_MILLIS;
        while (true) {
            try {
                return connectToAny();
            } catch (IllegalStateException e) {
                if (System.nanoTime() >= deadline) {
                    throw e;
                }
                log.warn("No metadata broker answered; retrying in {} ms", delay, e);
                Thread.sleep(delay);
                delay = nextBackoffMillis(delay);
            }
        }
    }

    /// The delay before the attempt after one that waited `currentMillis`: doubled, capped at 30 s.
    private static long nextBackoffMillis(long currentMillis) {
        return Math.min(currentMillis * 2, MAX_BACKOFF_MILLIS);
    }

    public Map<Short, VersionRange> brokerRanges() {
        return brokerRanges;
    }

    /// Requests the metadata once, outside the schedule; completes once the [MetadataCache] holds
    /// the response. Fails straight away when the metadata connection is not up (including while it
    /// is being re-established), and as soon as the connection closes (it drops or [#stop()] runs)
    /// while the request is in flight. A broker that stays connected but never answers leaves the
    /// stage incomplete, so callers still bound their wait.
    @Override
    public CompletionStage<Void> refresh() {
        Connection current = connection;
        if (!running || current == null || !current.channel().isActive()) {
            return CompletableFuture.failedFuture(new IllegalStateException("metadata connection is not active"));
        }
        var refreshed = new CompletableFuture<Void>();
        current.inFlightRefreshes().add(refreshed);
        refreshed.whenComplete((_, _) -> current.inFlightRefreshes().remove(refreshed));
        sendMetadata(current, frame -> {
            try {
                onMetadataResponse(frame);
                refreshed.complete(null);
            } catch (RuntimeException e) {
                refreshed.completeExceptionally(e);
                throw e;
            }
        });
        // The connection may have closed between the check above and the request being queued,
        // after the close listener already failed the in-flight refreshes.
        if (!current.channel().isActive()) {
            failInFlightRefreshes(current);
        }
        return refreshed;
    }

    /// Makes the connection `attempt` the current one, starts its refresh schedule and watches it
    /// for closing. A connection made while the client is stopping is closed straight away.
    private void onConnected(Connection attempt) {
        Channel channel = attempt.channel();
        connection = attempt;
        lastAnswered = attempt.endpoint();
        metrics.brokerConnectionOpened();
        channel.closeFuture().addListener(_ -> onClosed(attempt));
        if (!running) {
            channel.close();
            return;
        }
        log.info("Metadata client connected to {} remote {} active {} autoRead {}",
                attempt.endpoint(), channel.remoteAddress(), channel.isActive(), channel.config().isAutoRead());
        scheduledRefresh(attempt);
    }

    /// Runs on the connection's event loop when it closes. Drops it as the current connection and,
    /// while the client runs, starts the reconnect loop.
    private void onClosed(Connection closed) {
        metrics.brokerConnectionClosed();
        closed.pending().clear();
        failInFlightRefreshes(closed);
        if (connection == closed) {
            connection = null;
        }
        if (running) {
            log.warn("Metadata connection to {} lost; reconnecting", closed.endpoint());
            try {
                reconnector.execute(this::reconnect);
            } catch (RejectedExecutionException e) {
                // stop() shut the reconnect thread down between the check above and here.
            }
        }
    }

    /// Retries until a candidate answers or the client stops. Runs on the reconnect thread, never on
    /// an event loop, because connecting and the TLS and SASL handshakes block.
    private void reconnect() {
        long delay = INITIAL_BACKOFF_MILLIS;
        try {
            while (running) {
                try {
                    Connection attempt = connectToAny();
                    log.info("Metadata connection re-established");
                    onConnected(attempt);
                    return;
                } catch (IllegalStateException e) {
                    log.warn("No metadata broker answered; retrying in {} ms", delay, e);
                    Thread.sleep(delay);
                    delay = nextBackoffMillis(delay);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /// Tries each candidate once, in order, and returns the first connection that is established.
    /// When none is, throws an [IllegalStateException] whose cause is the last connect failure.
    private Connection connectToAny() throws InterruptedException {
        Exception lastFailure = null;
        for (Endpoint endpoint : candidates()) {
            try {
                return connect(endpoint);
            } catch (InterruptedException e) {
                throw e;
            } catch (Exception e) {
                log.warn("Metadata connect to {} failed ({})", endpoint, e.getMessage());
                lastFailure = e;
            }
        }
        throw new IllegalStateException("No metadata broker answered among " + candidates(), lastFailure);
    }

    /// The endpoints to try, in order: the broker that answered last, the bootstrap servers, then the
    /// brokers the cache last learned.
    private List<Endpoint> candidates() {
        var candidates = new LinkedHashSet<Endpoint>();
        Endpoint answered = lastAnswered;
        if (answered != null) {
            candidates.add(answered);
        }
        candidates.addAll(bootstrap);
        cache.brokers().forEach(broker -> candidates.add(new Endpoint(broker.host(), broker.port())));
        return List.copyOf(candidates);
    }

    private Connection connect(Endpoint endpoint) throws InterruptedException {
        var pending = new ConcurrentHashMap<Integer, Consumer<ByteBuf>>();
        var inFlightRefreshes = ConcurrentHashMap.<CompletableFuture<Void>>newKeySet();
        ChannelFuture connecting = new Bootstrap()
                .group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, CONNECT_TIMEOUT_MILLIS)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        settings.addTls(ch, endpoint.host(), endpoint.port());
                        ch.pipeline().addLast("frameDecoder", new KafkaFrameDecoder());
                        ch.pipeline().addLast("frameEncoder", new KafkaFrameEncoder());
                        ch.pipeline().addLast("handler", new MetadataResponseHandler(MetadataClient.this, pending));
                    }
                })
                .connect(endpoint.host(), endpoint.port());
        Channel channel;
        try {
            channel = connecting.sync().channel();
        } catch (InterruptedException e) {
            // stop() interrupted the reconnect thread mid-connect: do not leave the socket open.
            connecting.cancel(true);
            connecting.channel().close();
            throw e;
        }
        // establish() closes the channel itself when the TLS or SASL handshake fails.
        settings.establish(channel, codec, "metadata broker " + endpoint);
        return new Connection(endpoint, channel, pending, inFlightRefreshes);
    }

    private void failInFlightRefreshes(Connection closed) {
        var reason = new IllegalStateException("metadata connection to " + closed.endpoint() + " closed");
        closed.inFlightRefreshes().forEach(refresh -> refresh.completeExceptionally(reason));
    }

    private void scheduledRefresh(Connection conn) {
        if (!running || connection != conn || !conn.channel().isActive()) {
            return;
        }
        sendApiVersions(conn);
        sendMetadata(conn, this::onMetadataResponse);
        conn.channel().eventLoop().schedule(() -> scheduledRefresh(conn), REFRESH_SECONDS, TimeUnit.SECONDS);
    }

    private void sendApiVersions(Connection conn) {
        var request = new ApiVersionsRequestData()
                .setClientSoftwareName("kawa-gateway")
                .setClientSoftwareVersion("0.1.0");
        int correlationId = correlationIds.incrementAndGet();
        conn.pending().put(correlationId, this::onApiVersionsResponse);
        write(conn.channel(), API_VERSIONS, (short) 3, correlationId, request);
    }

    private void sendMetadata(Connection conn, Consumer<ByteBuf> onResponse) {
        // A null `topics` array requests metadata for ALL topics; an empty array requests none.
        var request = new MetadataRequestData().setTopics(null);
        int correlationId = correlationIds.incrementAndGet();
        conn.pending().put(correlationId, onResponse);
        write(conn.channel(), METADATA, (short) 8, correlationId, request);
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
        codec.encodeRequest(version, body, out);
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

    void handleFrame(Map<Integer, Consumer<ByteBuf>> pending, ByteBuf frame) {
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
        reconnector.shutdownNow();
        Connection current = connection;
        if (current != null) {
            current.channel().close();
        }
    }

    /// One established metadata connection and the response handlers of the requests sent on it.
    /// Closing a connection fails only its own in-flight refreshes, never those of a newer one.
    record Connection(Endpoint endpoint, Channel channel, Map<Integer, Consumer<ByteBuf>> pending,
                      Set<CompletableFuture<Void>> inFlightRefreshes) {
    }
}

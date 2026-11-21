package io.jonasg.kawa.server.broker;

import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.core.InterceptorPipeline;
import io.jonasg.kawa.core.metrics.GatewayMetrics;
import io.jonasg.kawa.protocol.kafka.KafkaBodyCodec;
import io.jonasg.kawa.protocol.kafka.KafkaFrameDecoder;
import io.jonasg.kawa.protocol.kafka.KafkaFrameEncoder;
import io.jonasg.kawa.protocol.kafka.KafkaHeader;
import io.jonasg.kawa.protocol.kafka.KafkaClientRequest;
import io.jonasg.kawa.protocol.kafka.KafkaResponse;
import io.jonasg.kawa.protocol.kafka.RequestHeaderCodec;
import io.jonasg.kawa.protocol.kafka.ResponseHeaderCodec;
import io.jonasg.kawa.server.netty.ClientSession;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.CorruptedFrameException;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/// A single upstream connection to one physical broker, owned by one client connection (see
/// [BrokerClientPool]). Requests are forwarded with a gateway-assigned correlation id.
public final class BrokerClient {

    private static final Logger log = LoggerFactory.getLogger(BrokerClient.class);
    private static final long REQUEST_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(30);
    /// Bounds each connect attempt, so a bootstrap server that drops packets does not stall the next target.
    private static final int CONNECT_TIMEOUT_MILLIS = 5_000;
    /// How long past the time a broker may legitimately hold a request the gateway still waits,
    /// as Kafka's own client does for JoinGroup.
    private static final long LONG_POLL_LAPSE_MILLIS = 5_000;

    private final int brokerId;
    private final List<Endpoint> targets;
    private final EventLoopGroup group;
    private final KafkaBodyCodec codec;
    private final InterceptorPipeline pipeline;
    private final GatewayMetrics metrics;
    private final BrokerConnectionSettings settings;
    private final RequestHeaderCodec requestHeaderCodec = new RequestHeaderCodec();
    private final ResponseHeaderCodec responseHeaderCodec = new ResponseHeaderCodec();
    private final Map<Integer, PendingRequest> pending = new ConcurrentHashMap<>();
    private final AtomicInteger correlationIds = new AtomicInteger();

    private volatile Channel channel;
    private volatile @Nullable Endpoint connectedTo;
    private volatile boolean closed;

    public BrokerClient(int brokerId, String host, int port, EventLoopGroup group,
                        KafkaBodyCodec codec, InterceptorPipeline pipeline, GatewayMetrics metrics) {
        this(brokerId, host, port, group, codec, pipeline, metrics, BrokerConnectionSettings.PLAINTEXT);
    }

    public BrokerClient(int brokerId, String host, int port, EventLoopGroup group,
                        KafkaBodyCodec codec, InterceptorPipeline pipeline, GatewayMetrics metrics,
                        BrokerConnectionSettings settings) {
        this(brokerId, List.of(new Endpoint(host, port)), group, codec, pipeline, metrics, settings);
    }

    /// @param targets the addresses to connect to, tried in order on each connect: one for a broker,
    ///                every bootstrap server for the bootstrap client
    BrokerClient(int brokerId, List<Endpoint> targets, EventLoopGroup group,
                 KafkaBodyCodec codec, InterceptorPipeline pipeline, GatewayMetrics metrics,
                 BrokerConnectionSettings settings) {
        this.brokerId = brokerId;
        this.targets = List.copyOf(targets);
        this.group = group;
        this.codec = codec;
        this.pipeline = pipeline;
        this.metrics = metrics;
        this.settings = settings;
    }

    public int brokerId() {
        return brokerId;
    }

    public boolean connected() {
        Channel ch = channel;
        return ch != null && ch.isActive();
    }

    /// Forwards a request to this broker. The caller must not touch the request afterwards.
    public void send(
            ClientSession session,
            GatewayContext context,
            KafkaClientRequest request
    ) {
        ensureConnected(session);
        int brokerCorrelationId = correlationIds.incrementAndGet();

        ScheduledFuture<?> timeoutTask = session.channel().eventLoop().schedule(
                () -> onTimeout(session, brokerCorrelationId),
                timeoutMillis(request.body()), TimeUnit.MILLISECONDS);
        var pendingRequest = new PendingRequest(
                session, context, request.header(), brokerCorrelationId, timeoutTask);
        pending.put(brokerCorrelationId, pendingRequest);

        KafkaHeader forwarded = KafkaHeader.of(
                (short) request.apiKey(), request.apiVersion(), brokerCorrelationId, request.clientId());
        ByteBuf out = channel.alloc().buffer();
        requestHeaderCodec.encode(out, forwarded);
        if (request.body() != null) {
            codec.encodeRequest(request.apiVersion(), request.body(), out);
        } else {
            out.writeBytes(request.rawBody());
            request.rawBody().release();
        }
        channel.writeAndFlush(out);
        metrics.request(request.apiName(), "forwarded");
        log.debug("Forwarded {} v{} (correlation {}) to broker {} on {}",
                request.apiName(), request.apiVersion(), brokerCorrelationId, brokerId, connectedTo);
    }

    void handleBrokerResponse(ByteBuf frame) {
        if (frame.readableBytes() < 4) {
            frame.release();
            return;
        }
        int correlationId = frame.getInt(frame.readerIndex());
        PendingRequest request = pending.remove(correlationId);
        if (request == null) {
            log.warn("Dropping response for unknown correlation id {} from broker {}", correlationId, brokerId);
            frame.release();
            return;
        }
        log.debug("Received response correlation {} ({} v{}) from broker {}",
                correlationId, request.apiName(), request.apiVersion(), brokerId);
        request.timeoutTask().cancel(false);
        responseHeaderCodec.decodeCorrelationId(frame, request.responseHeaderVersion());

        Object body = codec.decodeResponse(request.apiKey(), request.apiVersion(), frame);
        KafkaResponse response;
        if (body != null) {
            frame.release();
            response = KafkaResponse.of(KafkaHeader.of(request.apiKey(), request.apiVersion(),
                    request.requestHeader().correlationId(), request.requestHeader().clientId()), body);
        } else {
            response = new KafkaResponse(KafkaHeader.of(request.apiKey(), request.apiVersion(),
                    request.requestHeader().correlationId(), request.requestHeader().clientId()), null, frame);
        }

        ClientSession session = request.session();
        try {
            pipeline.onResponse(request.context(), response);
        } catch (RuntimeException e) {
            // Only this request's client pays: the broker connection is shared with other clients' requests.
            log.error("Failed to process {} v{} response (correlation {}) from broker {}; closing its client connection",
                    request.apiName(), request.apiVersion(), correlationId, brokerId, e);
            if (body == null) {
                frame.release();
            }
            session.close();
            metrics.response(response.apiName(), "error");
            return;
        }

        ByteBuf out = session.channel().alloc().buffer();
        responseHeaderCodec.encode(out, response.header().responseHeaderVersion(),
                request.requestHeader().correlationId());
        if (body != null) {
            codec.encodeResponse(request.apiVersion(), body, out);
        } else {
            out.writeBytes(frame);
            frame.release();
        }
        session.writeResponse(request.requestHeader().correlationId(), out);
        session.requestCompleted();
        metrics.response(response.apiName(), "ok");
        metrics.recordLatency(request.apiName(), System.nanoTime() - request.context().receivedAtNanos());
    }

    /// Closes the connection on purpose, e.g. because its client disconnected: unlike a broker
    /// dropping it, that is not worth a warning.
    public void close() {
        closed = true;
        Channel ch = channel;
        if (ch != null) {
            ch.close();
        }
        pending.values().forEach(p -> p.timeoutTask().cancel(false));
        pending.clear();
    }

    /// How long to wait for the broker's response. Some requests are long polls the broker holds on
    /// purpose: JoinGroup until every member has rejoined, for up to the rebalance timeout, and Fetch
    /// for up to its max wait. Those get that time plus a lapse, never less than the default.
    static long timeoutMillis(Object body) {
        long held = switch (body) {
            // v0 has no rebalance timeout (-1): the session timeout bounds the rebalance instead
            case JoinGroupRequestData join -> join.rebalanceTimeoutMs() >= 0 ? join.rebalanceTimeoutMs() : join.sessionTimeoutMs();
            case FetchRequestData fetch -> fetch.maxWaitMs();
            case null, default -> 0;
        };
        return Math.max(REQUEST_TIMEOUT_MILLIS, held + LONG_POLL_LAPSE_MILLIS);
    }

    private void onTimeout(
            ClientSession session,
            int brokerCorrelationId
    ) {
        PendingRequest request = pending.remove(brokerCorrelationId);
        if (request != null) {
            log.warn("Request {} (correlation {}) to broker {} timed out; closing client connection",
                    request.apiName(), brokerCorrelationId, brokerId);
            session.close();
        }
    }

    private void ensureConnected(ClientSession session) {
        Channel ch = channel;
        if (ch != null && ch.isActive()) {
            return;
        }
        try {
            connect();
        } catch (Exception e) {
            log.error("Failed to connect to broker {} at {}", brokerId, targets, e);
            session.close();
            throw new CorruptedFrameException("Broker connection failed", e);
        }
    }

    /// Connects to the first target that answers, trying them in order. Throws when none does.
    private void connect() throws InterruptedException {
        Channel ch = null;
        Endpoint answered = null;
        Exception lastFailure = null;
        for (Endpoint target : targets) {
            try {
                ch = open(target);
                answered = target;
                break;
            } catch (InterruptedException e) {
                throw e;
            } catch (Exception e) {
                log.warn("Connect to broker {} at {} failed ({}); trying the next target", brokerId, target, e.getMessage());
                lastFailure = e;
            }
        }
        if (ch == null) {
            throw new IllegalStateException("No target answered for broker " + brokerId + " among " + targets, lastFailure);
        }
        Channel connected = ch;
        this.channel = connected;
        this.connectedTo = answered;
        metrics.brokerConnectionOpened();
        connected.closeFuture().addListener(future -> {
            metrics.brokerConnectionClosed();
            Channel current = channel;
            if (current == connected) {
                channel = null;
            }
        });
    }

    /// Opens one connection to `target`, completing its TLS handshake and SASL login.
    private Channel open(Endpoint target) throws InterruptedException {
        var bootstrap = new Bootstrap();
        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, CONNECT_TIMEOUT_MILLIS)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        settings.addTls(ch, target.host(), target.port());
                        ch.pipeline().addLast("frameDecoder", new KafkaFrameDecoder());
                        ch.pipeline().addLast("frameEncoder", new KafkaFrameEncoder());
                        ch.pipeline().addLast("responseHandler", new BrokerResponseHandler(BrokerClient.this));
                    }
                });
        Channel ch = bootstrap.connect(target.host(), target.port()).sync().channel();
        settings.establish(ch, codec, "broker " + brokerId + " at " + target);
        return ch;
    }

    void onBrokerDisconnected() {
        if (closed) {
            return;
        }
        log.warn("Broker {} connection lost", brokerId);
        pending.forEach((id, request) -> {
            request.timeoutTask().cancel(false);
            request.session().close();
        });
        pending.clear();
    }
}

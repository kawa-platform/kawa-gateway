package io.jonasg.kawa.server.broker;

import io.jonasg.kawa.config.BrokerAuthConfig;
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
import io.jonasg.kawa.server.auth.BrokerSaslAuthenticator;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    /// How long past the time a broker may legitimately hold a request the gateway still waits,
    /// as Kafka's own client does for JoinGroup.
    private static final long LONG_POLL_LAPSE_MILLIS = 5_000;

    private final int brokerId;
    private final String host;
    private final int port;
    private final EventLoopGroup group;
    private final KafkaBodyCodec codec;
    private final InterceptorPipeline pipeline;
    private final GatewayMetrics metrics;
    private final BrokerAuthConfig brokerAuthConfig;
    private final RequestHeaderCodec requestHeaderCodec = new RequestHeaderCodec();
    private final ResponseHeaderCodec responseHeaderCodec = new ResponseHeaderCodec();
    private final Map<Integer, PendingRequest> pending = new ConcurrentHashMap<>();
    private final AtomicInteger correlationIds = new AtomicInteger();

    private volatile Channel channel;
    private volatile boolean closed;

    public BrokerClient(int brokerId, String host, int port, EventLoopGroup group,
                        KafkaBodyCodec codec, InterceptorPipeline pipeline, GatewayMetrics metrics) {
        this(brokerId, host, port, group, codec, pipeline, metrics, null);
    }

    public BrokerClient(int brokerId, String host, int port, EventLoopGroup group,
                        KafkaBodyCodec codec, InterceptorPipeline pipeline, GatewayMetrics metrics,
                        BrokerAuthConfig brokerAuthConfig) {
        this.brokerId = brokerId;
        this.host = host;
        this.port = port;
        this.group = group;
        this.codec = codec;
        this.pipeline = pipeline;
        this.metrics = metrics;
        this.brokerAuthConfig = brokerAuthConfig;
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
            codec.encodeRequest((short) request.apiKey(), request.apiVersion(), request.body(), out);
        } else {
            out.writeBytes(request.rawBody());
            request.rawBody().release();
        }
        channel.writeAndFlush(out);
        metrics.request(request.apiName(), "forwarded");
        log.debug("Forwarded {} v{} (correlation {}) to broker {} on {}:{}",
                request.apiName(), request.apiVersion(), brokerCorrelationId, brokerId, host, port);
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

        pipeline.onResponse(request.context(), response);

        ClientSession session = request.session();
        ByteBuf out = session.channel().alloc().buffer();
        responseHeaderCodec.encode(out, response.header().responseHeaderVersion(),
                request.requestHeader().correlationId());
        if (body != null) {
            codec.encodeResponse(request.apiKey(), request.apiVersion(), body, out);
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
            log.error("Failed to connect to broker {} at {}:{}", brokerId, host, port, e);
            session.close();
            throw new CorruptedFrameException("Broker connection failed", e);
        }
    }

    private void connect() throws InterruptedException {
        var bootstrap = new Bootstrap();
        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast("frameDecoder", new KafkaFrameDecoder());
                        ch.pipeline().addLast("frameEncoder", new KafkaFrameEncoder());
                        ch.pipeline().addLast("responseHandler", new BrokerResponseHandler(BrokerClient.this));
                    }
                });
        Channel ch = bootstrap.connect(host, port).sync().channel();
        if (brokerAuthConfig != null) {
            var authenticator = new BrokerSaslAuthenticator(brokerAuthConfig, codec);
            if (!authenticator.authenticate(ch)) {
                ch.close();
                throw new CorruptedFrameException("SASL authentication to broker " + brokerId + " failed");
            }
        }
        this.channel = ch;
        metrics.brokerConnectionOpened();
        ch.closeFuture().addListener(future -> {
            metrics.brokerConnectionClosed();
            Channel current = channel;
            if (current == ch) {
                channel = null;
            }
        });
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

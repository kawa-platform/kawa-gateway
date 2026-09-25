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
import io.jonasg.kawa.server.auth.BrokerSaslMechanism;
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
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/// A single upstream connection to one physical broker, shared by all client connections.
/// Requests are forwarded with a gateway-assigned correlation id so that correlation ids
/// from different clients never collide on the shared broker channel.
public final class BrokerClient {

    private static final Logger log = LoggerFactory.getLogger(BrokerClient.class);
    private static final long REQUEST_TIMEOUT_SECONDS = 30;

    private final int brokerId;
    private final String host;
    private final int port;
    private final EventLoopGroup group;
    private final KafkaBodyCodec codec;
    private final InterceptorPipeline pipeline;
    private final GatewayMetrics metrics;
    private final BrokerAuthConfig brokerAuthConfig;
    private final SslContext sslContext;
    private final RequestHeaderCodec requestHeaderCodec = new RequestHeaderCodec();
    private final ResponseHeaderCodec responseHeaderCodec = new ResponseHeaderCodec();
    private final Map<Integer, PendingRequest> pending = new ConcurrentHashMap<>();
    private final AtomicInteger correlationIds = new AtomicInteger();

    private volatile Channel channel;

    public BrokerClient(int brokerId, String host, int port, EventLoopGroup group,
                        KafkaBodyCodec codec, InterceptorPipeline pipeline, GatewayMetrics metrics) {
        this(brokerId, host, port, group, codec, pipeline, metrics, null);
    }

    public BrokerClient(int brokerId, String host, int port, EventLoopGroup group,
                         KafkaBodyCodec codec, InterceptorPipeline pipeline, GatewayMetrics metrics,
                         BrokerAuthConfig brokerAuthConfig) {
        this(brokerId, host, port, group, codec, pipeline, metrics, brokerAuthConfig, null);
    }

    public BrokerClient(int brokerId, String host, int port, EventLoopGroup group,
                        KafkaBodyCodec codec, InterceptorPipeline pipeline, GatewayMetrics metrics,
                        BrokerAuthConfig brokerAuthConfig, SslContext sslContext) {
        this.brokerId = brokerId;
        this.host = host;
        this.port = port;
        this.group = group;
        this.codec = codec;
        this.pipeline = pipeline;
        this.metrics = metrics;
        this.brokerAuthConfig = brokerAuthConfig;
        this.sslContext = sslContext;
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
                REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
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

    public void closeSession(ClientSession session) {
        pending.entrySet().removeIf(entry -> {
            if (entry.getValue().session() == session) {
                entry.getValue().timeoutTask().cancel(false);
                return true;
            }
            return false;
        });
    }

    public void close() {
        Channel ch = channel;
        if (ch != null) {
            ch.close();
        }
        pending.values().forEach(p -> p.timeoutTask().cancel(false));
        pending.clear();
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
                        if (sslContext != null) {
                            ch.pipeline().addLast("ssl", sslContext.newHandler(ch.alloc(), host, port));
                        }
                        ch.pipeline().addLast("frameDecoder", new KafkaFrameDecoder());
                        ch.pipeline().addLast("frameEncoder", new KafkaFrameEncoder());
                        ch.pipeline().addLast("responseHandler", new BrokerResponseHandler(BrokerClient.this));
                    }
                });
        Channel ch = bootstrap.connect(host, port).sync().channel();
        SslHandler sslHandler = ch.pipeline().get(SslHandler.class);
        if (sslHandler != null) {
            sslHandler.handshakeFuture().sync();
        }
        if (brokerAuthConfig != null) {
            var authenticator = new BrokerSaslAuthenticator(
                    BrokerSaslMechanism.from(brokerAuthConfig), codec, host);
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
        log.warn("Broker {} connection lost", brokerId);
        pending.forEach((id, request) -> {
            request.timeoutTask().cancel(false);
            request.session().close();
        });
        pending.clear();
    }
}

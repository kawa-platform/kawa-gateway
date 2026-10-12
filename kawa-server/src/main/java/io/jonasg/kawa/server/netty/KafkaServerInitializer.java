package io.jonasg.kawa.server.netty;

import io.jonasg.kawa.core.metrics.GatewayMetrics;
import io.jonasg.kawa.protocol.kafka.KafkaBodyCodec;
import io.jonasg.kawa.protocol.kafka.KafkaFrameDecoder;
import io.jonasg.kawa.protocol.kafka.KafkaFrameEncoder;
import io.jonasg.kawa.server.KafkaClientRequestHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslHandler;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.NullUnmarked;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public final class KafkaServerInitializer extends ChannelInitializer<SocketChannel> {

    private static final Logger log = LoggerFactory.getLogger(KafkaServerInitializer.class);

    private final DispatcherHolder dispatcherHolder;
    private final GatewayMetrics metrics;
    private final KafkaBodyCodec codec;
    private final SecurityProtocol securityProtocol;
    private final @Nullable ListenerTls tls;

    public KafkaServerInitializer(
            DispatcherHolder dispatcherHolder,
            GatewayMetrics metrics,
            KafkaBodyCodec codec,
            SecurityProtocol securityProtocol,
            @Nullable ListenerTls tls
    ) {
        this.dispatcherHolder = dispatcherHolder;
        this.metrics = metrics;
        this.codec = codec;
        this.securityProtocol = securityProtocol;
        this.tls = tls;
    }

    @Override
    protected void initChannel(SocketChannel ch) {
        KafkaClientRequestHandler dispatcher = dispatcherHolder.get();
        if (dispatcher == null) {
            ch.close();
            return;
        }
        var session = new ClientSession(ch, securityProtocol);
        if (tls != null) {
            var sslHandler = tls.newHandler(ch.alloc());
            ch.pipeline().addLast("tls", sslHandler);
            if (securityProtocol == SecurityProtocol.SSL) {
                // The handshake completes before any request is decrypted, so the principal is in
                // place before the first request reaches the dispatcher.
                sslHandler.handshakeFuture().addListener(handshake -> {
                    if (handshake.isSuccess()) {
                        authenticateWithCertificate(session, sslHandler);
                    }
                });
            }
        }
        ch.pipeline().addLast("frameDecoder", new KafkaFrameDecoder());
        ch.pipeline().addLast("frameEncoder", new KafkaFrameEncoder());
        ch.pipeline().addLast("handler", new KafkaClientConnectionHandler(dispatcher, session, codec));
        metrics.clientConnectionOpened();
        ch.closeFuture().addListener(_ -> metrics.clientConnectionClosed());
    }

    private void authenticateWithCertificate(ClientSession session, SslHandler sslHandler) {
        try {
            String principal = Objects.requireNonNull(tls).principal(sslHandler.engine().getSession());
            if (principal != null) {
                session.setPrincipal(principal);
            }
        } catch (IllegalStateException e) {
            log.warn("Closing client connection: {}", e.getMessage());
            session.close();
        }
    }

    /// Holds the dispatcher, which is created only after the listener is bound (port 0 resolution).
    public static final class DispatcherHolder {

        private volatile KafkaClientRequestHandler dispatcher;

        public void set(KafkaClientRequestHandler dispatcher) {
            this.dispatcher = dispatcher;
        }

        public KafkaClientRequestHandler get() {
            return dispatcher;
        }
    }
}

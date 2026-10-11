package io.jonasg.kawa.server.broker;

import io.jonasg.kawa.config.BrokerAuthConfig;
import io.jonasg.kawa.config.ClusterConfig;
import io.jonasg.kawa.protocol.kafka.KafkaBodyCodec;
import io.jonasg.kawa.server.auth.BrokerSaslAuthenticator;
import io.netty.channel.Channel;
import io.netty.handler.ssl.SslHandler;
import org.jspecify.annotations.Nullable;

import java.util.concurrent.TimeUnit;

/// How the gateway connects to the brokers: TLS and SASL, each optional (PLAINTEXT, SSL,
/// SASL_PLAINTEXT or SASL_SSL). Every broker connection is set up the same way: the TLS handler
/// goes first in the pipeline ([#addTls]), and once connected [#establish] waits for the TLS
/// handshake and then logs in with SASL, inside TLS.
///
/// @param auth the SASL login, or `null` for none
/// @param tls  TLS, or `null` for none
public record BrokerConnectionSettings(@Nullable BrokerAuthConfig auth, @Nullable BrokerTls tls) {

    /// No TLS and no SASL.
    public static final BrokerConnectionSettings PLAINTEXT = new BrokerConnectionSettings(null, null);

    private static final long HANDSHAKE_TIMEOUT_SECONDS = 10;

    /// The settings for `cluster`, building its TLS context once.
    public static BrokerConnectionSettings of(ClusterConfig cluster, @Nullable BrokerAuthConfig auth) {
        return new BrokerConnectionSettings(auth, cluster.tls() == null ? null : BrokerTls.from(cluster.tls()));
    }

    /// Adds the TLS handler as the first handler of a new connection to `host:port`, when TLS is on.
    public void addTls(Channel channel, String host, int port) {
        if (tls != null) {
            channel.pipeline().addFirst("tls", tls.newHandler(channel.alloc(), host, port));
        }
    }

    /// Completes a connected channel's TLS handshake and SASL login, closing it when either fails.
    ///
    /// @param broker names the broker in the error, e.g. `broker 2`
    /// @throws IllegalStateException when the handshake or the login fails
    public void establish(Channel channel, KafkaBodyCodec codec, String broker) {
        SslHandler handler = channel.pipeline().get(SslHandler.class);
        if (handler != null) {
            var handshake = handler.handshakeFuture().awaitUninterruptibly(HANDSHAKE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    ? handler.handshakeFuture()
                    : null;
            if (handshake == null || !handshake.isSuccess()) {
                channel.close();
                String reason = handshake == null ? "timed out" : rootMessage(handshake.cause());
                throw new IllegalStateException("TLS handshake with " + broker + " failed: " + reason,
                        handshake == null ? null : handshake.cause());
            }
        }
        if (auth != null && !new BrokerSaslAuthenticator(auth, codec).authenticate(channel)) {
            channel.close();
            throw new IllegalStateException("SASL authentication to " + broker + " failed");
        }
    }

    private static String rootMessage(Throwable error) {
        Throwable root = error;
        while (root.getCause() != null) {
            root = root.getCause();
        }
        return root.getMessage() != null ? root.getMessage() : root.toString();
    }
}

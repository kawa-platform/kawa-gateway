package io.jonasg.kawa.server.broker;

import io.jonasg.kawa.config.TlsConfig;
import io.jonasg.kawa.server.tls.TlsStores;
import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;

/// TLS for the gateway's own connections to the brokers: one client [SslContext], built once from
/// a cluster's [TlsConfig], that hands out an [SslHandler] per connection. The stores are read by
/// [TlsStores].
public final class BrokerTls {

    private final SslContext context;
    private final boolean verifyHostname;

    private BrokerTls(SslContext context, boolean verifyHostname) {
        this.context = context;
        this.verifyHostname = verifyHostname;
    }

    /// Loads the stores. Fails with the store's location and the reason when one cannot be read.
    public static BrokerTls from(TlsConfig config) {
        SslContextBuilder builder = SslContextBuilder.forClient();
        TlsStores.trust(builder, config.truststore());
        if (config.keystore() != null) {
            TlsStores.identity(builder, config.keystore());
        }
        try {
            return new BrokerTls(builder.build(), config.verifyHostname());
        } catch (Exception e) {
            throw new IllegalArgumentException("Cannot set up TLS to the brokers: " + e.getMessage(), e);
        }
    }

    /// A handler for a connection to `host`, checking the broker's certificate names that host
    /// unless hostname verification is off.
    public SslHandler newHandler(ByteBufAllocator alloc, String host, int port) {
        SslHandler handler = context.newHandler(alloc, host, port);
        if (verifyHostname) {
            SSLEngine engine = handler.engine();
            SSLParameters parameters = engine.getSSLParameters();
            parameters.setEndpointIdentificationAlgorithm("HTTPS");
            engine.setSSLParameters(parameters);
        }
        return handler;
    }
}

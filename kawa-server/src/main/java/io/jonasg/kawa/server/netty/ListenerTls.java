package io.jonasg.kawa.server.netty;

import io.jonasg.kawa.config.ClientAuth;
import io.jonasg.kawa.config.ListenerTlsConfig;
import io.jonasg.kawa.server.tls.TlsStores;
import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import org.apache.kafka.common.security.ssl.SslPrincipalMapper;
import org.jspecify.annotations.Nullable;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.security.auth.x500.X500Principal;
import java.io.IOException;
import java.security.Principal;

/// TLS on the client-facing listener: one server [SslContext], built once from the listener's
/// [ListenerTlsConfig], that hands out an [SslHandler] per client connection, and the mapping
/// from a client certificate to the principal of an `TLS` connection.
public final class ListenerTls {

    private final SslContext context;
    private final SslPrincipalMapper principalMapper;

    private ListenerTls(SslContext context, SslPrincipalMapper principalMapper) {
        this.context = context;
        this.principalMapper = principalMapper;
    }

    /// Loads the stores. Fails with the store's location and the reason when one cannot be read.
    public static ListenerTls from(ListenerTlsConfig config) {
        SslContextBuilder builder = TlsStores.forServer(config.keystore())
                .clientAuth(clientAuth(config.clientAuth()));
        if (config.truststore() != null) {
            TlsStores.trust(builder, config.truststore());
        }
        try {
            return new ListenerTls(builder.build(), SslPrincipalMapper.fromRules(config.principalMappingRules()));
        } catch (Exception e) {
            throw new IllegalArgumentException("Cannot set up TLS on the client listener: " + e.getMessage(), e);
        }
    }

    /// A handler for a new client connection.
    public SslHandler newHandler(ByteBufAllocator alloc) {
        return context.newHandler(alloc);
    }

    /// The principal of the client certificate presented in `session`, mapped by the listener's
    /// principal mapping rules, or `null` when the client presented none.
    ///
    /// @throws IllegalStateException when no mapping rule matches the certificate's subject
    public @Nullable String principal(SSLSession session) {
        Principal peer;
        try {
            peer = session.getPeerPrincipal();
        } catch (SSLPeerUnverifiedException e) {
            return null;
        }
        if (!(peer instanceof X500Principal x500)) {
            return null;
        }
        try {
            return principalMapper.getName(x500.getName());
        } catch (IOException e) {
            throw new IllegalStateException("Cannot map client certificate '" + x500.getName() + "' to a principal: "
                    + e.getMessage(), e);
        }
    }

    private static io.netty.handler.ssl.ClientAuth clientAuth(ClientAuth clientAuth) {
        return switch (clientAuth) {
            case NONE -> io.netty.handler.ssl.ClientAuth.NONE;
            case REQUESTED -> io.netty.handler.ssl.ClientAuth.OPTIONAL;
            case REQUIRED -> io.netty.handler.ssl.ClientAuth.REQUIRE;
        };
    }
}

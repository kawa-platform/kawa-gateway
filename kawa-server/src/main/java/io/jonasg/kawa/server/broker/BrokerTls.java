package io.jonasg.kawa.server.broker;

import io.jonasg.kawa.config.StoreConfig;
import io.jonasg.kawa.config.StoreType;
import io.jonasg.kawa.config.TlsConfig;
import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManagerFactory;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/// TLS for the gateway's own connections to the brokers: one client [SslContext], built once from
/// a cluster's [TlsConfig], that hands out an [SslHandler] per connection. Accepts PKCS12 and JKS
/// stores, and PEM files (CA certificates as a truststore; a private key with its certificate
/// chain as a keystore, as Kafka expects them).
public final class BrokerTls {

    private static final Pattern PEM_BLOCK = Pattern.compile(
            "-----BEGIN ([A-Z ]+)-----.*?-----END \\1-----", Pattern.DOTALL);

    private final SslContext context;
    private final boolean verifyHostname;

    private BrokerTls(SslContext context, boolean verifyHostname) {
        this.context = context;
        this.verifyHostname = verifyHostname;
    }

    /// Loads the stores. Fails with the store's location and the reason when one cannot be read.
    public static BrokerTls from(TlsConfig config) {
        SslContextBuilder builder = SslContextBuilder.forClient();
        trust(builder, config.truststore());
        if (config.keystore() != null) {
            identity(builder, config.keystore());
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

    private static void trust(SslContextBuilder builder, StoreConfig store) {
        try {
            if (store.type() == StoreType.PEM) {
                builder.trustManager(new File(store.location()));
                return;
            }
            var factory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            factory.init(load(store));
            builder.trustManager(factory);
        } catch (Exception e) {
            throw new IllegalArgumentException("Cannot load TLS truststore '" + store.location() + "': " + e.getMessage(), e);
        }
    }

    private static void identity(SslContextBuilder builder, StoreConfig store) {
        try {
            if (store.type() == StoreType.PEM) {
                pemIdentity(builder, store);
                return;
            }
            String keyPassword = store.keyPassword() != null ? store.keyPassword() : store.password();
            var factory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            factory.init(load(store), keyPassword.toCharArray());
            builder.keyManager(factory);
        } catch (Exception e) {
            throw new IllegalArgumentException("Cannot load TLS keystore '" + store.location() + "': " + e.getMessage(), e);
        }
    }

    /// A PEM keystore holds the private key and its certificate chain in one file.
    private static void pemIdentity(SslContextBuilder builder, StoreConfig store) throws Exception {
        String pem = Files.readString(Path.of(store.location()));
        var certificates = new StringBuilder();
        String key = null;
        Matcher block = PEM_BLOCK.matcher(pem);
        while (block.find()) {
            if (block.group(1).equals("CERTIFICATE")) {
                certificates.append(block.group()).append('\n');
            } else if (block.group(1).endsWith("PRIVATE KEY")) {
                key = block.group();
            }
        }
        if (key == null || certificates.isEmpty()) {
            throw new IllegalArgumentException("a PEM keystore needs a private key and its certificate chain in one file");
        }
        builder.keyManager(stream(certificates.toString()), stream(key), store.keyPassword());
    }

    private static InputStream stream(String pem) {
        return new ByteArrayInputStream(pem.getBytes(StandardCharsets.US_ASCII));
    }

    private static KeyStore load(StoreConfig store) throws Exception {
        KeyStore keyStore = KeyStore.getInstance(store.type().name());
        try (InputStream in = Files.newInputStream(Path.of(store.location()))) {
            keyStore.load(in, store.password().toCharArray());
        }
        return keyStore;
    }
}

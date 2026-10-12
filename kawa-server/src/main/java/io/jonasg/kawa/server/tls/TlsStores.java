package io.jonasg.kawa.server.tls;

import io.jonasg.kawa.config.StoreConfig;
import io.jonasg.kawa.config.StoreType;
import io.netty.handler.ssl.SslContextBuilder;

import javax.net.ssl.KeyManagerFactory;
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

/// Reads the TLS key and trust stores of a [StoreConfig] into Netty [SslContextBuilder]s, the same
/// way for the gateway's connections to the brokers and for its client-facing listener. Accepts
/// PKCS12 and JKS stores, and PEM files (CA certificates as a truststore; a private key with its
/// certificate chain as a keystore, as Kafka expects them).
///
/// Fails with the store's location and the reason when a store cannot be read.
public final class TlsStores {

    private static final Pattern PEM_BLOCK = Pattern.compile(
            "-----BEGIN ([A-Z ]+)-----.*?-----END \\1-----", Pattern.DOTALL);

    private TlsStores() {
    }

    /// A server context builder presenting the certificate and key of `keystore`.
    public static SslContextBuilder forServer(StoreConfig keystore) {
        try {
            if (keystore.type() == StoreType.PEM) {
                PemIdentity identity = pemIdentity(keystore);
                return SslContextBuilder.forServer(stream(identity.certificates()), stream(identity.key()), keystore.keyPassword());
            }
            return SslContextBuilder.forServer(keyManagerFactory(keystore));
        } catch (Exception e) {
            throw keystoreError(keystore, e);
        }
    }

    /// Makes `builder` present the certificate and key of `keystore`.
    public static void identity(SslContextBuilder builder, StoreConfig keystore) {
        try {
            if (keystore.type() == StoreType.PEM) {
                PemIdentity identity = pemIdentity(keystore);
                builder.keyManager(stream(identity.certificates()), stream(identity.key()), keystore.keyPassword());
                return;
            }
            builder.keyManager(keyManagerFactory(keystore));
        } catch (Exception e) {
            throw keystoreError(keystore, e);
        }
    }

    /// Makes `builder` verify the peer's certificate against the CA certificates of `truststore`.
    public static void trust(SslContextBuilder builder, StoreConfig truststore) {
        try {
            if (truststore.type() == StoreType.PEM) {
                builder.trustManager(new File(truststore.location()));
                return;
            }
            var factory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            factory.init(load(truststore));
            builder.trustManager(factory);
        } catch (Exception e) {
            throw new IllegalArgumentException("Cannot load TLS truststore '" + truststore.location() + "': " + e.getMessage(), e);
        }
    }

    private static KeyManagerFactory keyManagerFactory(StoreConfig store) throws Exception {
        String keyPassword = store.keyPassword() != null ? store.keyPassword() : store.password();
        var factory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        factory.init(load(store), keyPassword.toCharArray());
        return factory;
    }

    private static IllegalArgumentException keystoreError(StoreConfig store, Exception e) {
        return new IllegalArgumentException("Cannot load TLS keystore '" + store.location() + "': " + e.getMessage(), e);
    }

    /// A PEM keystore holds the private key and its certificate chain in one file.
    private static PemIdentity pemIdentity(StoreConfig store) throws Exception {
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
        return new PemIdentity(certificates.toString(), key);
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

    private record PemIdentity(String certificates, String key) {
    }
}

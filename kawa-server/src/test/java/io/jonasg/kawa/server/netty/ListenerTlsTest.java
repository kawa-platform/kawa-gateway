package io.jonasg.kawa.server.netty;

import io.jonasg.kawa.config.ClientAuth;
import io.jonasg.kawa.config.ListenerTlsConfig;
import io.jonasg.kawa.config.StoreConfig;
import io.jonasg.kawa.config.StoreType;
import io.jonasg.kawa.test.TestTls;
import io.netty.buffer.ByteBufAllocator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.security.auth.x500.X500Principal;
import java.lang.reflect.Proxy;
import java.security.Principal;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ListenerTlsTest {

    private static StoreConfig store(String fixture, StoreType type) {
        String password = type == StoreType.PEM ? null : TestTls.PASSWORD;
        return new StoreConfig(TestTls.file(fixture).toString(), type, password, null);
    }

    static Stream<Arguments> keystores() {
        return Stream.of(
                Arguments.of("PKCS12", store("broker.p12", StoreType.PKCS12)),
                Arguments.of("JKS", store("broker.jks", StoreType.JKS)),
                Arguments.of("PEM", store("broker.pem", StoreType.PEM)),
                Arguments.of("encrypted PEM key", new StoreConfig(
                        TestTls.file("broker-encrypted.pem").toString(), StoreType.PEM, null, TestTls.PASSWORD)));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("keystores")
    void servesTlsFromEveryKeystoreFormat(String format, StoreConfig keystore) {
        // when
        var tls = ListenerTls.from(new ListenerTlsConfig(keystore, null, null, null));

        // then
        var engine = tls.newHandler(ByteBufAllocator.DEFAULT).engine();
        assertThat(engine.getUseClientMode())
                .withFailMessage(() -> "The " + format + " TLS handler is not in server mode")
                .isFalse();
        assertThat(engine.getNeedClientAuth() || engine.getWantClientAuth())
                .withFailMessage(() -> "Clients are asked for a certificate although clientAuth is NONE")
                .isFalse();
    }

    @Test
    void requiresOrRequestsAClientCertificate() {
        // given
        var keystore = store("broker.p12", StoreType.PKCS12);
        var truststore = store("ca.pem", StoreType.PEM);

        // when
        var required = ListenerTls.from(new ListenerTlsConfig(keystore, truststore, ClientAuth.REQUIRED, null))
                .newHandler(ByteBufAllocator.DEFAULT).engine();
        var requested = ListenerTls.from(new ListenerTlsConfig(keystore, truststore, ClientAuth.REQUESTED, null))
                .newHandler(ByteBufAllocator.DEFAULT).engine();

        // then
        assertThat(required.getNeedClientAuth())
                .withFailMessage(() -> "A client certificate is not required with clientAuth REQUIRED")
                .isTrue();
        assertThat(requested.getWantClientAuth())
                .withFailMessage(() -> "A client certificate is not requested with clientAuth REQUESTED")
                .isTrue();
        assertThat(requested.getNeedClientAuth()).isFalse();
    }

    @Test
    void mapsTheClientCertificateToItsCnByDefault() {
        // given
        var tls = ListenerTls.from(new ListenerTlsConfig(store("broker.p12", StoreType.PKCS12), null, null, null));

        // when
        String principal = tls.principal(sessionWithPeer("CN=alice,OU=payments,O=acme"));

        // then
        assertThat(principal)
                .withFailMessage(() -> "Expected the certificate's CN as principal, got " + principal)
                .isEqualTo("alice");
    }

    @Test
    void mapsTheClientCertificateWithCustomRules() {
        // given
        var tls = ListenerTls.from(new ListenerTlsConfig(store("broker.p12", StoreType.PKCS12), null, null,
                "RULE:^CN=([^,]+),OU=([^,]+).*$/$2-$1/L,DEFAULT"));

        // when
        String principal = tls.principal(sessionWithPeer("CN=Alice,OU=Payments,O=acme"));

        // then
        assertThat(principal).isEqualTo("payments-alice");
    }

    @Test
    void hasNoPrincipalWithoutAClientCertificate() {
        // given
        var tls = ListenerTls.from(new ListenerTlsConfig(store("broker.p12", StoreType.PKCS12), null, null, null));
        // when / then
        assertThat(tls.principal(sessionWithPeer(null)))
                .withFailMessage(() -> "A connection without a client certificate got a principal")
                .isNull();
    }

    @Test
    void namesTheKeystoreWhenItCannotBeOpened() {
        // given
        String keystore = TestTls.file("broker.p12").toString();

        // when / then
        assertThatThrownBy(() -> ListenerTls.from(new ListenerTlsConfig(
                new StoreConfig(keystore, StoreType.PKCS12, "wrong-password", null), null, null, null)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot load TLS keystore '" + keystore + "'");
    }

    /// A TLS session whose client presented a certificate for `subject`, or none when it is `null`.
    private static SSLSession sessionWithPeer(String subject) {
        return (SSLSession) Proxy.newProxyInstance(SSLSession.class.getClassLoader(), new Class<?>[]{SSLSession.class},
                (_, method, _) -> {
                    if (!method.getName().equals("getPeerPrincipal")) {
                        throw new UnsupportedOperationException(method.getName());
                    }
                    if (subject == null) {
                        throw new SSLPeerUnverifiedException("no client certificate");
                    }
                    Principal peer = new X500Principal(subject);
                    return peer;
                });
    }
}

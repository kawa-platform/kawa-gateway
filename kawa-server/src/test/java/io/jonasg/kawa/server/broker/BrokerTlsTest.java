package io.jonasg.kawa.server.broker;

import io.jonasg.kawa.config.StoreConfig;
import io.jonasg.kawa.config.StoreType;
import io.jonasg.kawa.config.TlsConfig;
import io.jonasg.kawa.test.TestTls;
import io.netty.buffer.ByteBufAllocator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BrokerTlsTest {

    private static StoreConfig store(String fixture, StoreType type) {
        String password = type == StoreType.PEM ? null : TestTls.PASSWORD;
        return new StoreConfig(TestTls.file(fixture).toString(), type, password, null);
    }

    static Stream<Arguments> stores() {
        return Stream.of(
                Arguments.of("PKCS12", store("truststore.p12", StoreType.PKCS12), store("gateway.p12", StoreType.PKCS12)),
                Arguments.of("JKS", store("truststore.jks", StoreType.JKS), store("gateway.jks", StoreType.JKS)),
                Arguments.of("PEM", store("ca.pem", StoreType.PEM), store("gateway.pem", StoreType.PEM)),
                Arguments.of("encrypted PEM key", store("ca.pem", StoreType.PEM), new StoreConfig(
                        TestTls.file("gateway-encrypted.pem").toString(), StoreType.PEM, null, TestTls.PASSWORD)));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("stores")
    void setsUpTlsFromEveryStoreFormat(String format, StoreConfig truststore, StoreConfig keystore) {
        // when
        var tls = BrokerTls.from(new TlsConfig(truststore, keystore, true));

        // then
        var engine = tls.newHandler(ByteBufAllocator.DEFAULT, "localhost", 9093).engine();
        assertThat(engine.getUseClientMode())
                .withFailMessage(() -> "The " + format + " TLS handler is not in client mode")
                .isTrue();
        assertThat(engine.getSSLParameters().getEndpointIdentificationAlgorithm())
                .withFailMessage(() -> "Hostname verification is not on for " + format)
                .isEqualTo("HTTPS");
    }

    @Test
    void skipsHostnameVerificationWhenTurnedOff() {
        // given
        var tls = BrokerTls.from(new TlsConfig(store("ca.pem", StoreType.PEM), null, false));

        // when
        var engine = tls.newHandler(ByteBufAllocator.DEFAULT, "localhost", 9093).engine();

        // then
        assertThat(engine.getSSLParameters().getEndpointIdentificationAlgorithm())
                .withFailMessage(() -> "Hostname verification is on although it was turned off")
                .isNull();
    }

    @Test
    void namesTheStoreWhenItCannotBeOpened() {
        // given
        String truststore = TestTls.file("truststore.p12").toString();

        // when / then
        assertThatThrownBy(() -> BrokerTls.from(new TlsConfig(
                new StoreConfig(truststore, StoreType.PKCS12, "wrong-password", null), null, true)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot load TLS truststore '" + truststore + "'");
    }
}

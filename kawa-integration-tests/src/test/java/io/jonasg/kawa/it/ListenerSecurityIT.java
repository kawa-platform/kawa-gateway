package io.jonasg.kawa.it;

import io.jonasg.kawa.config.AclConfig;
import io.jonasg.kawa.config.AuthConfig;
import io.jonasg.kawa.config.ClientAuth;
import io.jonasg.kawa.config.GroupConfig;
import io.jonasg.kawa.config.ListenerConfig;
import io.jonasg.kawa.config.ListenerTlsConfig;
import io.jonasg.kawa.config.RbacConfig;
import io.jonasg.kawa.config.ResourceConfig;
import io.jonasg.kawa.config.RoleConfig;
import io.jonasg.kawa.config.StoreConfig;
import io.jonasg.kawa.config.StoreType;
import io.jonasg.kawa.server.KafkaGateway;
import io.jonasg.kawa.test.TestTls;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.requests.SaslHandshakeRequest;
import org.apache.kafka.common.requests.SaslHandshakeResponse;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// The security protocols of the gateway's client listener, end to end with real Kafka clients:
/// `SSL` with the principal taken from the client certificate, `SASL_SSL` with PLAIN and SCRAM,
/// and a `PLAINTEXT` listener refusing SASL the way a broker does.
///
/// Each test starts its own gateway in front of the shared broker and config topic. The gateway
/// presents the `broker` test certificate, which names `localhost`; clients trust the test CA. The
/// `gateway` client certificate has CN `gateway`, which becomes its principal on `SSL`.
class ListenerSecurityIT extends GatewayTestSupport {

    private static final String CERTIFICATE_PRINCIPAL = "gateway";
    private static final String SCRAM_USER = "scram-alice";
    private static final String SCRAM_PASSWORD = "scram-secret";

    private KafkaGateway secured;

    @AfterEach
    void stopSecuredGateway() {
        if (secured != null) {
            secured.stop();
            secured = null;
        }
    }

    @Override
    protected AuthConfig authConfig() {
        return new AuthConfig(
                Set.of("PLAIN", "SCRAM-SHA-256"),
                Map.of(DEFAULT_PRINCIPAL, client("PLAIN", DEFAULT_PASSWORD),
                        SCRAM_USER, client("SCRAM-SHA-256", SCRAM_PASSWORD)));
    }

    @Override
    protected RbacConfig rbacConfig() {
        var allowAll = new RoleConfig(List.of(
                new AclConfig(new ResourceConfig(ResourceType.TOPIC, "", PatternType.PREFIXED), AclOperation.ALL),
                new AclConfig(new ResourceConfig(ResourceType.GROUP, "", PatternType.PREFIXED), AclOperation.ALL),
                new AclConfig(new ResourceConfig(ResourceType.CLUSTER, null), AclOperation.ALL)));
        return new RbacConfig(
                Map.of("allow-all", allowAll),
                Map.of("clients", new GroupConfig(List.of(DEFAULT_PRINCIPAL, SCRAM_USER, CERTIFICATE_PRINCIPAL),
                        List.of("allow-all"))));
    }

    @Test
    void servesSslClientsAuthenticatedByTheirCertificate() throws Exception {
        // given
        String bootstrap = startSecured(SecurityProtocol.SSL, mutualTls(ClientAuth.REQUIRED));

        // when / then
        assertRoundTrip(sslProps(bootstrap, true), "ssl-mtls");
    }

    @Test
    void refusesAnSslClientWithoutACertificateWhenOneIsRequired() throws Exception {
        // given
        String bootstrap = startSecured(SecurityProtocol.SSL, mutualTls(ClientAuth.REQUIRED));

        // when / then
        assertFailsToConnect(sslProps(bootstrap, false));
    }

    @Test
    void deniesAnSslClientThatPresentsNoCertificate() throws Exception {
        // given: a certificate is only requested, so the client connects but has no principal
        String bootstrap = startSecured(SecurityProtocol.SSL, mutualTls(ClientAuth.REQUESTED));

        // when / then
        try (var admin = AdminClient.create(withTimeouts(sslProps(bootstrap, false)))) {
            assertThatThrownBy(() -> admin.createTopics(List.of(new NewTopic("ssl-anonymous", 1, (short) 1)))
                    .all().get(30, TimeUnit.SECONDS))
                    .withFailMessage(() -> "A client without a certificate, and so without a principal, created a topic")
                    .isInstanceOf(ExecutionException.class);
        }
    }

    @Test
    void servesSaslSslClientsWithPlain() throws Exception {
        // given
        String bootstrap = startSecured(SecurityProtocol.SASL_SSL, serverTls("broker.pem", StoreType.PEM));

        // when / then
        assertRoundTrip(saslSslProps(bootstrap, "PLAIN", DEFAULT_PRINCIPAL, DEFAULT_PASSWORD), "sasl-ssl-plain");
    }

    @Test
    void servesSaslSslClientsWithScram() throws Exception {
        // given
        String bootstrap = startSecured(SecurityProtocol.SASL_SSL, serverTls("broker.p12", StoreType.PKCS12));

        // when / then
        assertRoundTrip(saslSslProps(bootstrap, "SCRAM-SHA-256", SCRAM_USER, SCRAM_PASSWORD), "sasl-ssl-scram");
    }

    @Test
    void refusesAClientThatDoesNotTrustTheGatewaysCertificate() throws Exception {
        // given
        String bootstrap = startSecured(SecurityProtocol.SASL_SSL, serverTls("broker.p12", StoreType.PKCS12));
        Properties props = saslSslProps(bootstrap, "PLAIN", DEFAULT_PRINCIPAL, DEFAULT_PASSWORD);
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, TestTls.file("other-ca.pem").toString());
        props.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PEM");
        props.remove(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);

        // when / then
        assertFailsToConnect(props);
    }

    @Test
    void refusesAPlaintextClientOnASaslSslListener() throws Exception {
        // given
        String bootstrap = startSecured(SecurityProtocol.SASL_SSL, serverTls("broker.p12", StoreType.PKCS12));

        // when / then
        assertFailsToConnect(saslProps(bootstrap));
    }

    @Test
    void answersSaslHandshakeWithIllegalSaslStateOnAPlaintextListener() throws Exception {
        // given
        String bootstrap = startSecured(SecurityProtocol.PLAINTEXT, null);

        // when
        SaslHandshakeResponse response = sendHandshake(bootstrap);

        // then
        assertThat(response.data().errorCode())
                .withFailMessage(() -> "SaslHandshake on a PLAINTEXT listener was not refused with ILLEGAL_SASL_STATE, got "
                        + Errors.forCode(response.data().errorCode()))
                .isEqualTo(Errors.ILLEGAL_SASL_STATE.code());
    }

    private String startSecured(SecurityProtocol protocol, ListenerTlsConfig tls) throws Exception {
        secured = new KafkaGateway(bootstrapConfig(new ListenerConfig("127.0.0.1", 0, protocol, tls)));
        secured.start();
        return "localhost:" + secured.boundPort();
    }

    private static ListenerTlsConfig serverTls(String keystore, StoreType type) {
        return new ListenerTlsConfig(store(keystore, type), null, null, null);
    }

    private static ListenerTlsConfig mutualTls(ClientAuth clientAuth) {
        return new ListenerTlsConfig(store("broker.p12", StoreType.PKCS12), store("truststore.p12", StoreType.PKCS12),
                clientAuth, null);
    }

    private static StoreConfig store(String fixture, StoreType type) {
        return new StoreConfig(TestTls.file(fixture).toString(), type, type == StoreType.PEM ? null : TestTls.PASSWORD, null);
    }

    /// A client of an `SSL` listener trusting the test CA, with the `gateway` certificate when
    /// `withCertificate`.
    private static Properties sslProps(String bootstrap, boolean withCertificate) {
        var props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(SECURITY_PROTOCOL_CONFIG, "SSL");
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, TestTls.file("truststore.p12").toString());
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, TestTls.PASSWORD);
        if (withCertificate) {
            props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, TestTls.file("gateway.p12").toString());
            props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, TestTls.PASSWORD);
        }
        return props;
    }

    private static Properties saslSslProps(String bootstrap, String mechanism, String username, String password) {
        var props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, TestTls.file("ca.pem").toString());
        props.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PEM");
        props.put(SaslConfigs.SASL_MECHANISM, mechanism);
        String loginModule = mechanism.equals("PLAIN")
                ? "org.apache.kafka.common.security.plain.PlainLoginModule"
                : "org.apache.kafka.common.security.scram.ScramLoginModule";
        props.put(SaslConfigs.SASL_JAAS_CONFIG,
                loginModule + " required username=\"" + username + "\" password=\"" + password + "\";");
        return props;
    }

    private static Properties withTimeouts(Properties props) {
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5_000);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 10_000);
        return props;
    }

    private static void assertFailsToConnect(Properties props) {
        try (var admin = AdminClient.create(withTimeouts(props))) {
            assertThatThrownBy(() -> admin.describeCluster().clusterId().get(30, TimeUnit.SECONDS))
                    .withFailMessage(() -> "The client reached the gateway although its connection should have been refused")
                    .isInstanceOf(ExecutionException.class);
        }
    }

    private static void assertRoundTrip(Properties clientProps, String topic) throws Exception {
        try (var admin = AdminClient.create(clientProps)) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(30, TimeUnit.SECONDS);
        }
        var producerProps = new Properties();
        producerProps.putAll(clientProps);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        try (var producer = new KafkaProducer<String, String>(producerProps)) {
            producer.send(new ProducerRecord<>(topic, "key", "secured")).get(30, TimeUnit.SECONDS);
        }
        var consumerProps = new Properties();
        consumerProps.putAll(clientProps);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        try (var consumer = new KafkaConsumer<String, String>(consumerProps)) {
            var partition = new TopicPartition(topic, 0);
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            var values = new ArrayList<String>();
            long deadline = System.currentTimeMillis() + 30_000;
            while (values.isEmpty() && System.currentTimeMillis() < deadline) {
                consumer.poll(Duration.ofMillis(500)).forEach(record -> values.add(record.value()));
            }
            assertThat(values)
                    .withFailMessage(() -> "The record produced to '" + topic + "' through the secured listener was not consumed")
                    .containsExactly("secured");
        }
    }

    private static SaslHandshakeResponse sendHandshake(String bootstrap) throws Exception {
        String[] parts = bootstrap.split(":");
        try (Socket socket = new Socket(parts[0], Integer.parseInt(parts[1]))) {
            socket.setSoTimeout(5000);
            RequestHeader header = new RequestHeader(ApiKeys.SASL_HANDSHAKE, (short) 1, "kawa-raw-it", 1);
            var request = new SaslHandshakeRequest(new SaslHandshakeRequestData().setMechanism("PLAIN"), (short) 1);
            ByteBuffer payload = request.serializeWithHeader(header);

            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            out.writeInt(payload.remaining());
            out.write(payload.array(), payload.position(), payload.remaining());
            out.flush();

            DataInputStream in = new DataInputStream(socket.getInputStream());
            byte[] responseBytes = new byte[in.readInt()];
            in.readFully(responseBytes);
            ByteBuffer buffer = ByteBuffer.wrap(responseBytes);
            ResponseHeader.parse(buffer, ApiKeys.SASL_HANDSHAKE.responseHeaderVersion((short) 1));
            return SaslHandshakeResponse.parse(new ByteBufferAccessor(buffer), (short) 1);
        }
    }
}

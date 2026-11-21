package io.jonasg.kawa.virtualtopic.filter;

import com.sun.net.httpserver.HttpServer;
import io.jonasg.kawa.config.SchemaRegistryBasicAuthConfig;
import org.apache.avro.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Runs against a stub registry on the JDK's [HttpServer].
class SchemaRegistryAvroSchemaResolverTest {

    private static final String ORDER_SCHEMA =
            "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"status\",\"type\":\"string\"}]}";

    private HttpServer registry;
    private final List<String> authorizations = new CopyOnWriteArrayList<>();
    private final List<String> paths = new CopyOnWriteArrayList<>();
    private volatile int status = 200;
    private volatile String body;

    @BeforeEach
    void startRegistry() throws IOException {
        registry = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        registry.createContext("/", exchange -> {
            paths.add(exchange.getRequestURI().getRawPath());
            String authorization = exchange.getRequestHeaders().getFirst("Authorization");
            authorizations.add(authorization == null ? "" : authorization);
            byte[] response = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/vnd.schemaregistry.v1+json");
            exchange.sendResponseHeaders(status, response.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(response);
            }
        });
        registry.start();
    }

    @AfterEach
    void stopRegistry() {
        registry.stop(0);
    }

    @Test
    void resolvesASchemaById() {
        // given
        body = schemaResponse(ORDER_SCHEMA);
        var resolver = resolver(null);

        // when
        Schema schema = resolver.resolve(42);

        // then
        assertThat(paths)
                .withFailMessage(() -> "The registry was not asked for /schemas/ids/42, requests were " + paths)
                .containsExactly("/schemas/ids/42");
        assertThat(schema.getFullName())
                .withFailMessage(() -> "The registry's Order schema was not parsed, got " + schema)
                .isEqualTo("Order");
        assertThat(authorizations)
                .withFailMessage(() -> "An Authorization header was sent without basic auth configured")
                .containsExactly("");
    }

    @Test
    void sendsBasicAuth() {
        // given
        body = schemaResponse(ORDER_SCHEMA);
        var resolver = resolver(new SchemaRegistryBasicAuthConfig("kawa", "s3cret"));

        // when
        resolver.resolve(1);

        // then
        String expected = "Basic " + Base64.getEncoder().encodeToString("kawa:s3cret".getBytes(StandardCharsets.UTF_8));
        assertThat(authorizations)
                .withFailMessage(() -> "The basic auth credentials were not sent, got " + authorizations)
                .containsExactly(expected);
    }

    @Test
    void reportsTheRegistryErrorForAnUnknownId() {
        // given
        status = 404;
        body = "{\"error_code\":40403,\"message\":\"Schema 9 not found\"}";
        var resolver = resolver(null);

        // when / then
        assertThatThrownBy(() -> resolver.resolve(9))
                .withFailMessage(() -> "An unknown schema id did not fail with the registry's error")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("HTTP 404")
                .hasMessageContaining("Schema 9 not found");
    }

    @Test
    void rejectsANonAvroSchema() {
        // given
        body = "{\"schemaType\":\"PROTOBUF\",\"schema\":\"syntax = \\\"proto3\\\";\"}";
        var resolver = resolver(null);

        // when / then
        assertThatThrownBy(() -> resolver.resolve(3))
                .withFailMessage(() -> "A Protobuf schema was not rejected")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("PROTOBUF");
    }

    @Test
    void rejectsASchemaWithReferences() {
        // given
        body = "{\"schema\":\"\\\"string\\\"\",\"references\":[{\"name\":\"Customer\",\"subject\":\"customer\",\"version\":1}]}";
        var resolver = resolver(null);

        // when / then
        assertThatThrownBy(() -> resolver.resolve(4))
                .withFailMessage(() -> "A schema with references was not rejected")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("references");
    }

    @Test
    void reportsAnUnreachableRegistryAsUnavailable() {
        // given
        var resolver = resolver(null);
        registry.stop(0);

        // when / then
        assertThatThrownBy(() -> resolver.resolve(1))
                .withFailMessage(() -> "An unreachable registry was not reported as unavailable")
                .isInstanceOf(SchemaRegistryUnavailableException.class)
                .hasMessageContaining("unreachable");
    }

    @Test
    void reportsATimedOutRegistryAsUnavailable() throws IOException {
        // given a registry that accepts the connection but never answers
        try (var silent = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            var resolver = new SchemaRegistryAvroSchemaResolver(
                    "http://127.0.0.1:" + silent.getLocalPort(), null, HttpClient.newHttpClient());

            // when / then
            assertThatThrownBy(() -> resolver.resolve(1))
                    .withFailMessage(() -> "A registry that did not answer in time was not reported as unavailable")
                    .isInstanceOf(SchemaRegistryUnavailableException.class);
        }
    }

    @Test
    void reportsAServerErrorAsUnavailable() {
        // given
        status = 503;
        body = "{\"error_code\":50301,\"message\":\"Service Unavailable\"}";
        var resolver = resolver(null);

        // when / then
        assertThatThrownBy(() -> resolver.resolve(1))
                .withFailMessage(() -> "An HTTP 503 from the registry was not reported as unavailable")
                .isInstanceOf(SchemaRegistryUnavailableException.class)
                .hasMessageContaining("HTTP 503");
    }

    @Test
    void reportsAServerErrorWithoutAJsonBodyAsUnavailable() {
        // given a proxy in front of the registry answering with an HTML error page
        status = 502;
        body = "<html><body>Bad Gateway</body></html>";
        var resolver = resolver(null);

        // when / then
        assertThatThrownBy(() -> resolver.resolve(1))
                .withFailMessage(() -> "An HTTP 502 with an HTML body was not reported as unavailable")
                .isInstanceOf(SchemaRegistryUnavailableException.class)
                .hasMessageContaining("HTTP 502");
    }

    @Test
    void reportsAServerErrorForASubjectAsUnavailable() {
        // given
        status = 500;
        body = "{\"error_code\":50001,\"message\":\"Error in the backend datastore\"}";
        var resolver = resolver(null);

        // when / then
        assertThatThrownBy(() -> resolver.resolve("orders-value", null))
                .withFailMessage(() -> "An HTTP 500 for a subject lookup was not reported as unavailable")
                .isInstanceOf(SchemaRegistryUnavailableException.class)
                .hasMessageContaining("HTTP 500");
    }

    @Test
    void doesNotReportAnUnknownSchemaAsUnavailable() {
        // given
        status = 404;
        body = "{\"error_code\":40403,\"message\":\"Schema 9 not found\"}";
        var resolver = resolver(null);

        // when / then
        assertThatThrownBy(() -> resolver.resolve(9))
                .withFailMessage(() -> "An unknown schema id was reported as a registry outage")
                .isNotInstanceOf(SchemaRegistryUnavailableException.class);
        assertThatThrownBy(() -> resolver.resolve("nope", null))
                .withFailMessage(() -> "An unknown subject was reported as a registry outage")
                .isNotInstanceOf(SchemaRegistryUnavailableException.class);
    }

    @Test
    void doesNotReportAnInvalidSchemaAsUnavailable() {
        // given
        body = "{\"schema\":\"{not avro\"}";
        var resolver = resolver(null);

        // when / then
        assertThatThrownBy(() -> resolver.resolve(3))
                .withFailMessage(() -> "An invalid Avro schema was reported as a registry outage")
                .isNotInstanceOf(SchemaRegistryUnavailableException.class);
    }

    @Test
    void resolvesTheLatestVersionOfASubject() {
        // given
        body = "{\"subject\":\"orders-value\",\"version\":3,\"id\":17," + schemaResponse(ORDER_SCHEMA).substring(1);
        var resolver = resolver(null);

        // when
        RegisteredAvroSchema registered = resolver.resolve("orders-value", null);

        // then
        assertThat(paths)
                .withFailMessage(() -> "The registry was not asked for the latest version, requests were " + paths)
                .containsExactly("/subjects/orders-value/versions/latest");
        assertThat(registered.id())
                .withFailMessage(() -> "The subject's schema id was not taken from the response, got " + registered.id())
                .isEqualTo(17);
        assertThat(registered.schema().getFullName())
                .withFailMessage(() -> "The subject's Order schema was not parsed, got " + registered.schema())
                .isEqualTo("Order");
    }

    @Test
    void resolvesAnExplicitVersionOfAnEncodedSubject() {
        // given
        body = "{\"id\":5," + schemaResponse(ORDER_SCHEMA).substring(1);
        var resolver = resolver(null);

        // when
        resolver.resolve("orders value/eu", 2);

        // then
        assertThat(paths)
                .withFailMessage(() -> "The subject was not URL-encoded or the version not asked for, requests were " + paths)
                .containsExactly("/subjects/orders%20value%2Feu/versions/2");
    }

    @Test
    void reportsTheRegistryErrorForAnUnknownSubject() {
        // given
        status = 404;
        body = "{\"error_code\":40401,\"message\":\"Subject 'nope' not found.\"}";
        var resolver = resolver(null);

        // then
        assertThatThrownBy(() -> resolver.resolve("nope", null))
                .hasMessageContaining("HTTP 404")
                .hasMessageContaining("subject 'nope' version latest")
                .hasMessageContaining("Subject 'nope' not found.");
    }

    @Test
    void rejectsASubjectResponseWithoutAnId() {
        // given
        body = schemaResponse(ORDER_SCHEMA);
        var resolver = resolver(null);

        // then
        assertThatThrownBy(() -> resolver.resolve("orders-value", null))
                .hasMessageContaining("has no schema id");
    }

    private SchemaRegistryAvroSchemaResolver resolver(SchemaRegistryBasicAuthConfig basicAuth) {
        String url = "http://127.0.0.1:" + registry.getAddress().getPort() + "/";
        return new SchemaRegistryAvroSchemaResolver(url, basicAuth, HttpClient.newHttpClient());
    }

    private static String schemaResponse(String schema) {
        return "{\"schema\":\"" + schema.replace("\"", "\\\"") + "\"}";
    }
}

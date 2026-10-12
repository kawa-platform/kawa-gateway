package io.jonasg.kawa.virtualtopic.filter;

import com.sun.net.httpserver.HttpServer;
import io.jonasg.kawa.config.SchemaRegistryBasicAuthConfig;
import org.apache.avro.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
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
            paths.add(exchange.getRequestURI().getPath());
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
    void reportsAnUnreachableRegistry() {
        // given
        var resolver = resolver(null);
        registry.stop(0);

        // when / then
        assertThatThrownBy(() -> resolver.resolve(1))
                .withFailMessage(() -> "An unreachable registry did not fail the lookup")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("unreachable");
    }

    private SchemaRegistryAvroSchemaResolver resolver(SchemaRegistryBasicAuthConfig basicAuth) {
        String url = "http://127.0.0.1:" + registry.getAddress().getPort() + "/";
        return new SchemaRegistryAvroSchemaResolver(url, basicAuth, HttpClient.newHttpClient());
    }

    private static String schemaResponse(String schema) {
        return "{\"schema\":\"" + schema.replace("\"", "\\\"") + "\"}";
    }
}

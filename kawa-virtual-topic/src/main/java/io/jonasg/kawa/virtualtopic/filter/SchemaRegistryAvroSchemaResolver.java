package io.jonasg.kawa.virtualtopic.filter;

import io.jonasg.kawa.config.SchemaRegistryBasicAuthConfig;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.jspecify.annotations.Nullable;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;

/// Resolves Avro schemas from a Confluent-compatible schema registry with
/// `GET {url}/schemas/ids/{id}`.
///
/// Every call goes to the registry; wrap it in a [CachingAvroSchemaResolver]. Schemas that use
/// schema references are rejected for now.
public final class SchemaRegistryAvroSchemaResolver implements AvroSchemaResolver {

    private static final JsonMapper MAPPER = JsonMapper.builder().build();
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(5);

    private final String baseUrl;
    private final @Nullable String authorization;
    private final HttpClient httpClient;

    /// @param url        the registry's base URL
    /// @param basicAuth  HTTP basic authentication, or `null` for none
    /// @param httpClient the client to call the registry with, configured with the TLS the
    ///                   registry needs
    public SchemaRegistryAvroSchemaResolver(String url, @Nullable SchemaRegistryBasicAuthConfig basicAuth, HttpClient httpClient) {
        this.baseUrl = url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
        this.authorization = basicAuth == null ? null : "Basic " + Base64.getEncoder().encodeToString(
                (basicAuth.username() + ":" + basicAuth.password()).getBytes(StandardCharsets.UTF_8));
        this.httpClient = httpClient;
    }

    @Override
    public Schema resolve(int schemaId) {
        HttpResponse<String> response = get(schemaId);
        JsonNode body = parse(schemaId, response.body());
        if (response.statusCode() != 200) {
            throw new IllegalStateException("schema registry returned HTTP " + response.statusCode()
                    + " for schema id " + schemaId + errorMessage(body));
        }
        String schemaType = text(body, "schemaType");
        if (schemaType != null && !schemaType.equals("AVRO")) {
            throw new IllegalStateException("schema id " + schemaId + " is a " + schemaType + " schema, not Avro");
        }
        JsonNode references = body.path("references");
        if (references.isArray() && !references.isEmpty()) {
            throw new IllegalStateException("schema id " + schemaId + " uses schema references, which are not supported yet");
        }
        String schema = text(body, "schema");
        if (schema == null) {
            throw new IllegalStateException("schema registry response for schema id " + schemaId + " has no schema");
        }
        try {
            return new Schema.Parser().parse(schema);
        } catch (SchemaParseException e) {
            throw new IllegalStateException("schema id " + schemaId + " is not a valid Avro schema: " + e.getMessage(), e);
        }
    }

    private HttpResponse<String> get(int schemaId) {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(baseUrl + "/schemas/ids/" + schemaId))
                .timeout(REQUEST_TIMEOUT)
                .header("Accept", "application/vnd.schemaregistry.v1+json, application/json")
                .GET();
        if (authorization != null) {
            request.header("Authorization", authorization);
        }
        try {
            return httpClient.send(request.build(), HttpResponse.BodyHandlers.ofString());
        } catch (IOException e) {
            throw new IllegalStateException("schema registry at " + baseUrl + " is unreachable: " + e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while fetching schema id " + schemaId, e);
        }
    }

    private static JsonNode parse(int schemaId, String body) {
        try {
            return MAPPER.readTree(body);
        } catch (JacksonException e) {
            throw new IllegalStateException("schema registry response for schema id " + schemaId + " is not JSON", e);
        }
    }

    private static String errorMessage(JsonNode body) {
        String message = text(body, "message");
        return message == null ? "" : ": " + message;
    }

    private static @Nullable String text(JsonNode node, String field) {
        JsonNode value = node.path(field);
        return value.isString() ? value.stringValue() : null;
    }
}

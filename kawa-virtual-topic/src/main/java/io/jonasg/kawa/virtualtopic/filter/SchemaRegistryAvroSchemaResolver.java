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
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;

/// Resolves Avro schemas from a Confluent-compatible schema registry: by id with
/// `GET {url}/schemas/ids/{id}`, and by subject with `GET {url}/subjects/{subject}/versions/{version}`.
///
/// Every call goes to the registry; wrap it in a [CachingAvroSchemaResolver] or
/// [CachingAvroSubjectResolver]. Schemas that use schema references are rejected for now.
///
/// A registry that is unreachable, does not answer within [#REQUEST_TIMEOUT] or answers with an
/// HTTP 5xx error fails with a [SchemaRegistryUnavailableException]: an outage, not a record
/// error. Any other failure (an unknown id or subject, an invalid schema) is an
/// [IllegalStateException].
public final class SchemaRegistryAvroSchemaResolver implements AvroSchemaResolver, AvroSubjectResolver {

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
        String what = "schema id " + schemaId;
        return schema(what, fetch("/schemas/ids/" + schemaId, what));
    }

    @Override
    public RegisteredAvroSchema resolve(String subject, @Nullable Integer version) {
        String versionPath = version == null ? "latest" : version.toString();
        String what = "subject '" + subject + "' version " + versionPath;
        JsonNode body = fetch("/subjects/" + URLEncoder.encode(subject, StandardCharsets.UTF_8).replace("+", "%20")
                + "/versions/" + versionPath, what);
        JsonNode id = body.path("id");
        if (!id.isInt()) {
            throw new IllegalStateException("schema registry response for " + what + " has no schema id");
        }
        return new RegisteredAvroSchema(id.intValue(), schema(what, body));
    }

    /// GETs `path` and returns the JSON body of a `200` response.
    private JsonNode fetch(String path, String what) {
        HttpResponse<String> response = get(path, what);
        if (response.statusCode() >= 500 && response.statusCode() < 600) {
            // Checked before parsing: a proxy in front of a failing registry answers with HTML.
            throw new SchemaRegistryUnavailableException("schema registry at " + baseUrl + " returned HTTP " + response.statusCode()
                    + " for " + what);
        }
        JsonNode body = parse(what, response.body());
        if (response.statusCode() != 200) {
            throw new IllegalStateException("schema registry returned HTTP " + response.statusCode()
                    + " for " + what + errorMessage(body));
        }
        return body;
    }

    /// Parses the Avro schema of a registry response that describes one schema.
    private static Schema schema(String what, JsonNode body) {
        String schemaType = text(body, "schemaType");
        if (schemaType != null && !schemaType.equals("AVRO")) {
            throw new IllegalStateException(what + " is a " + schemaType + " schema, not Avro");
        }
        JsonNode references = body.path("references");
        if (references.isArray() && !references.isEmpty()) {
            throw new IllegalStateException(what + " uses schema references, which are not supported yet");
        }
        String schema = text(body, "schema");
        if (schema == null) {
            throw new IllegalStateException("schema registry response for " + what + " has no schema");
        }
        try {
            return new Schema.Parser().parse(schema);
        } catch (SchemaParseException e) {
            throw new IllegalStateException(what + " is not a valid Avro schema: " + e.getMessage(), e);
        }
    }

    private HttpResponse<String> get(String path, String what) {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(baseUrl + path))
                .timeout(REQUEST_TIMEOUT)
                .header("Accept", "application/vnd.schemaregistry.v1+json, application/json")
                .GET();
        if (authorization != null) {
            request.header("Authorization", authorization);
        }
        try {
            return httpClient.send(request.build(), HttpResponse.BodyHandlers.ofString());
        } catch (HttpTimeoutException e) {
            throw new SchemaRegistryUnavailableException("schema registry at " + baseUrl + " is unreachable: no answer for " + what
                    + " within " + REQUEST_TIMEOUT.toSeconds() + "s", e);
        } catch (IOException e) {
            throw new SchemaRegistryUnavailableException("schema registry at " + baseUrl + " is unreachable: " + e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SchemaRegistryUnavailableException("interrupted while fetching " + what
                    + " from the schema registry at " + baseUrl + ", so it is treated as unreachable", e);
        }
    }

    private static JsonNode parse(String what, String body) {
        try {
            return MAPPER.readTree(body);
        } catch (JacksonException e) {
            throw new IllegalStateException("schema registry response for " + what + " is not JSON", e);
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

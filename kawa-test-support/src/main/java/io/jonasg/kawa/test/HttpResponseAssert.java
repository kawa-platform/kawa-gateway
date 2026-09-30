package io.jonasg.kawa.test;

import net.javacrumbs.jsonunit.assertj.JsonAssertions;
import org.assertj.core.api.AbstractAssert;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.SerializationFeature;
import tools.jackson.databind.json.JsonMapper;

import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/// AssertJ assertions for an [HttpResponse] with a `String` body. A failing assertion prints the
/// request line, the response headers and the (pretty-printed when JSON) body, so a wrong status
/// shows *why* the server answered the way it did instead of just `expected: 200 but was: 400`.
public final class HttpResponseAssert extends AbstractAssert<HttpResponseAssert, HttpResponse<String>> {

    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final JsonMapper PRETTY_JSON = JsonMapper.builder()
            .enable(SerializationFeature.INDENT_OUTPUT)
            .build();

    HttpResponseAssert(HttpResponse<String> actual) {
        super(actual, HttpResponseAssert.class);
    }

    public HttpResponseAssert hasStatusCode(int expected) {
        isNotNull();
        if (actual.statusCode() != expected) {
            failWithActualExpectedAndMessage(
                    actual.statusCode(),
                    expected,
                    "%nExpected status code <%s> but was <%s>%n%s",
                    expected,
                    actual.statusCode(),
                    describe(actual));
        }
        return this;
    }

    /// Asserts that the JSON body (or every element, when the body is an array) has exactly
    /// these top-level property names — no more, no fewer.
    public HttpResponseAssert containsExactlyTopLevelPropertyNames(String... names) {
        isNotNull();

        var expected = Set.of(names);
        JsonNode root = JSON.readTree(actual.body());
        var elements = new ArrayList<JsonNode>();
        if (root.isArray()) {
            root.forEach(elements::add);
        } else {
            elements.add(root);
        }
        if (elements.isEmpty()) {
            failWithMessage("response body contains no elements: %s", actual.body());
        }
        for (var element : elements) {
            var actualNames = new TreeSet<String>();
            for (var property : element.properties()) {
                actualNames.add(property.getKey());
            }
            if (!actualNames.equals(expected)) {
                failWithMessage(
                        "wire property names %s != expected %s in: %s",
                        actualNames,
                        expected,
                        actual.body());
            }
        }
        return this;
    }

    /// Asserts the response body matches `expectedBody`, leniently.
    ///
    /// A non-JSON `expectedBody` is compared as an exact string.
    public HttpResponseAssert hasBody(String expectedBody) {
        JsonAssertions.assertThatJson(actual.body())
                .isEqualTo(expectedBody);
        return this;
    }

    private static String describe(HttpResponse<String> response) {
        var request = response.request();
        var headers = response.headers().map().entrySet().stream()
                .map(header -> "    " + header.getKey() + ": " + String.join(", ", header.getValue()))
                .collect(Collectors.joining("%n".formatted()));
        return """
                request:
                    %s %s
                response headers:
                %s
                response body:
                %s""".formatted(
                request.method(),
                request.uri(),
                headers.isEmpty() ? "    <none>" : headers,
                indent(prettyBody(response.body())));
    }

    private static String prettyBody(String body) {
        if (body == null || body.isBlank()) {
            return "<empty>";
        }
        try {
            return PRETTY_JSON.writeValueAsString(JSON.readTree(body));
        } catch (JacksonException e) {
            return body;
        }
    }

    private static String indent(String text) {
        return text.lines().map(line -> "    " + line).collect(Collectors.joining("%n".formatted()));
    }
}

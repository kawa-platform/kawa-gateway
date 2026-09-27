package io.jonasg.kawa.http;

import org.assertj.core.api.AbstractAssert;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Set;
import java.util.TreeSet;

public final class HttpResponseAssert extends AbstractAssert<HttpResponseAssert, HttpResponse<String>> {

    private static final JsonMapper JSON = JsonMapper.builder().build();

    HttpResponseAssert(HttpResponse<String> actual) {
        super(actual, HttpResponseAssert.class);
    }

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
}

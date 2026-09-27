package io.jonasg.kawa.http;

import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLSession;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import java.util.Optional;

import static io.jonasg.kawa.http.KawaAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KawaAssertionsTest {

    @Test
    void passesWhenAnObjectResponseHasExactlyTheExpectedPropertyNames() {
        // given a role body with the expected properties
        var response = new StubResponse("{\"name\":\"reader\",\"acls\":[]}");

        // then
        assertThat(response).containsExactlyTopLevelPropertyNames("name", "acls");
    }

    @Test
    void passesWhenEveryArrayElementHasExactlyTheExpectedPropertyNames() {
        // given a list body whose elements share the expected properties
        var response = new StubResponse("""
                [{"name":"reader","acls":[]},{"name":"writer","acls":[]}]""");

        // then
        assertThat(response).containsExactlyTopLevelPropertyNames("name", "acls");
    }

    @Test
    void failsWhenAResponseCarriesAnUnexpectedProperty() {
        // given a body with a property beside the expected ones
        var response = new StubResponse("{\"name\":\"reader\",\"acls\":[],\"unexpected\":true}");

        // then the failure names the offending key set
        assertThatThrownBy(() -> assertThat(response).containsExactlyTopLevelPropertyNames("name", "acls"))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("[acls, name, unexpected]")
                .hasMessageContaining("[acls, name]");
    }

    @Test
    void failsWhenOneArrayElementDoesNotHaveTheExpectedProperties() {
        // given a list whose second element misses a property
        var response = new StubResponse("""
                [{"name":"reader","acls":[]},{"name":"writer"}]""");

        // then
        assertThatThrownBy(() -> assertThat(response).containsExactlyTopLevelPropertyNames("name", "acls"))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("[name]");
    }

    /// A response carrying only a body, which is all [HttpResponseAssert] inspects.
    private record StubResponse(String body) implements HttpResponse<String> {

        @Override
        public int statusCode() {
            return 200;
        }

        @Override
        public HttpRequest request() {
            return HttpRequest.newBuilder(URI.create("http://localhost")).build();
        }

        @Override
        public Optional<HttpResponse<String>> previousResponse() {
            return Optional.empty();
        }

        @Override
        public HttpHeaders headers() {
            return HttpHeaders.of(Map.of(), (name, value) -> true);
        }

        @Override
        public Optional<SSLSession> sslSession() {
            return Optional.empty();
        }

        @Override
        public URI uri() {
            return URI.create("http://localhost");
        }

        @Override
        public HttpClient.Version version() {
            return HttpClient.Version.HTTP_1_1;
        }
    }
}

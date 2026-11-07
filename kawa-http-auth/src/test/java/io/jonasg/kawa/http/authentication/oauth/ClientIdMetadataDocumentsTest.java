package io.jonasg.kawa.http.authentication.oauth;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicInteger;

import static io.jonasg.kawa.test.KawaAssertions.assertThat;

/// Reading a client's metadata document from a real (loopback) HTTP server.
class ClientIdMetadataDocumentsTest {

    private static final String CALLBACK = "http://localhost:*/callback";

    private final AtomicInteger requests = new AtomicInteger();
    private final MutableClock clock = new MutableClock();
    private HttpServer server;
    private URI url;
    private int status = 200;
    private String body;
    private String location;
    private Duration delay = Duration.ZERO;

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        url = URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/client.json");
        body = document(url.toString(), "\"redirect_uris\": [\"" + CALLBACK + "\"]");
        server.createContext("/client.json", exchange -> {
            requests.incrementAndGet();
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (location != null) {
                exchange.getResponseHeaders().add("Location", location);
            }
            var bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });
        server.start();
    }

    @AfterEach
    void stopServer() {
        server.stop(0);
    }

    @Test
    void readsTheRedirectUrisOfAValidDocument() {
        // when
        var document = documents().fetch(url);

        // then
        assertThat(document).withFailMessage(() -> "a valid document was not accepted").isPresent();
        assertThat(document.get().redirectUris()).hasSize(1);
        assertThat(document.get().redirectUris().getFirst().allows(URI.create("http://localhost:43123/callback")))
                .withFailMessage(() -> "the loopback any-port redirect URI of the document was not honoured")
                .isTrue();
    }

    @Test
    void refusesADocumentWhoseClientIdIsNotItsUrl() {
        // given
        body = document("https://elsewhere.example/client.json", "\"redirect_uris\": [\"" + CALLBACK + "\"]");

        // when
        var document = documents().fetch(url);

        // then
        assertThat(document).withFailMessage(() -> "a document naming another client_id was accepted").isEmpty();
    }

    @Test
    void refusesADocumentWithoutRedirectUris() {
        // given
        body = "{\"client_id\": \"" + url + "\"}";

        // when
        var document = documents().fetch(url);

        // then
        assertThat(document).withFailMessage(() -> "a document without redirect_uris was accepted").isEmpty();
    }

    @Test
    void refusesADocumentWithAnInvalidRedirectUri() {
        // given
        body = document(url.toString(), "\"redirect_uris\": [\"not a uri\"]");

        // when
        var document = documents().fetch(url);

        // then
        assertThat(document).withFailMessage(() -> "a document with an invalid redirect_uri was accepted").isEmpty();
    }

    @Test
    void refusesADocumentThatAsksForClientAuthentication() {
        // given
        body = document(url.toString(), "\"redirect_uris\": [\"" + CALLBACK + "\"], \"token_endpoint_auth_method\": \"client_secret_basic\"");

        // when
        var document = documents().fetch(url);

        // then
        assertThat(document).withFailMessage(() -> "a document asking for client_secret_basic was accepted").isEmpty();
    }

    @Test
    void acceptsADocumentThatNamesNoneAsItsAuthenticationMethod() {
        // given
        body = document(url.toString(), "\"redirect_uris\": [\"" + CALLBACK + "\"], \"token_endpoint_auth_method\": \"none\"");

        // when
        var document = documents().fetch(url);

        // then
        assertThat(document).withFailMessage(() -> "a public client document was refused").isPresent();
    }

    @Test
    void refusesADocumentThatIsNotJson() {
        // given
        body = "<html>nope</html>";

        // when
        var document = documents().fetch(url);

        // then
        assertThat(document).withFailMessage(() -> "an HTML page was accepted as a document").isEmpty();
    }

    @Test
    void refusesAnOversizedDocument() {
        // given
        body = document(url.toString(), "\"redirect_uris\": [\"" + CALLBACK + "\"], \"padding\": \""
                + "x".repeat(ClientIdMetadataDocuments.MAX_BODY_BYTES) + "\"");

        // when
        var document = documents().fetch(url);

        // then
        assertThat(document).withFailMessage(() -> "a document over the size cap was accepted").isEmpty();
    }

    @Test
    void doesNotFollowARedirect() throws IOException {
        // given: the URL redirects to a valid document
        var target = server.getAddress().getPort();
        server.createContext("/real.json", exchange -> {
            requests.incrementAndGet();
            var bytes = document(url.toString(), "\"redirect_uris\": [\"" + CALLBACK + "\"]").getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });
        status = 302;
        location = "http://127.0.0.1:" + target + "/real.json";
        body = "";

        // when
        var document = documents().fetch(url);

        // then
        assertThat(document).withFailMessage(() -> "a redirect was followed to a document").isEmpty();
        assertThat(requests.get()).withFailMessage(() -> "the redirect target was requested").isEqualTo(1);
    }

    @Test
    void refusesANonSuccessResponse() {
        // given
        status = 404;

        // when
        var document = documents().fetch(url);

        // then
        assertThat(document).withFailMessage(() -> "a 404 answer was accepted").isEmpty();
    }

    @Test
    void givesUpOnASlowHost() {
        // given
        delay = Duration.ofSeconds(2);

        // when
        var document = new ClientIdMetadataDocuments(clock, UrlPolicy.LOOPBACK_FOR_TESTS, Duration.ofMillis(200)).fetch(url);

        // then
        assertThat(document).withFailMessage(() -> "a host slower than the timeout was waited for").isEmpty();
    }

    @Test
    void cachesADocumentUntilItsTtlPasses() {
        // given
        var documents = documents();
        documents.fetch(url);

        // when
        documents.fetch(url);
        int beforeTtl = requests.get();
        clock.advance(ClientIdMetadataDocuments.TTL.plusSeconds(1));
        documents.fetch(url);

        // then
        assertThat(beforeTtl).withFailMessage(() -> "the second fetch was not served from the cache").isEqualTo(1);
        assertThat(requests.get()).withFailMessage(() -> "the document was not fetched again after its TTL").isEqualTo(2);
    }

    @Test
    void cachesAFailureBrieflyAndThenRetries() {
        // given
        status = 500;
        var documents = documents();
        documents.fetch(url);
        documents.fetch(url);
        int whileCached = requests.get();
        status = 200;

        // when
        clock.advance(ClientIdMetadataDocuments.FAILURE_TTL.plusSeconds(1));
        var document = documents.fetch(url);

        // then
        assertThat(whileCached).withFailMessage(() -> "a failing host was contacted again within the failure TTL").isEqualTo(1);
        assertThat(document).withFailMessage(() -> "the recovered host was not read after the failure TTL").isPresent();
    }

    @Test
    void showsTheClientNameAsPlainTextWithoutControlCharactersAndCapped() {
        // given: a name with a bidirectional override, a newline and a tab, and then a very long one
        body = document(url.toString(), "\"client_name\": \"  Evil\\u202e App\\n\\tName  \", \"redirect_uris\": [\"" + CALLBACK + "\"]");
        var cleaned = documents().fetch(url);
        clock.advance(ClientIdMetadataDocuments.TTL.plusSeconds(1));
        body = document(url.toString(), "\"client_name\": \"" + "n".repeat(500) + "\", \"redirect_uris\": [\"" + CALLBACK + "\"]");
        var capped = documents().fetch(url);

        // then
        assertThat(cleaned.get().clientName()).withFailMessage(() -> "control characters were kept in the name").isEqualTo("Evil AppName");
        assertThat(capped.get().clientName()).withFailMessage(() -> "an overlong name was not capped")
                .hasSize(ClientIdMetadataDocuments.MAX_NAME_LENGTH);
    }

    @Test
    void hasNoNameWhenTheDocumentGivesNoneOrOnlyControlCharacters() {
        // given
        body = document(url.toString(), "\"client_name\": \"\\u0007\\u202e\", \"redirect_uris\": [\"" + CALLBACK + "\"]");

        // when
        var document = documents().fetch(url);

        // then
        assertThat(document.get().clientName()).withFailMessage(() -> "a name of control characters was kept").isNull();
    }

    @Test
    void neverFetchesAnHttpUrlOrALoopbackHostUnderTheProductionPolicy() {
        // given: the server is reachable, but only on plain http at a loopback address
        var production = new ClientIdMetadataDocuments(clock, UrlPolicy.PUBLIC_HTTPS);
        var https = URI.create("https://localhost:" + server.getAddress().getPort() + "/client.json");

        // when
        var overHttp = production.fetch(url);
        var loopbackOverHttps = production.fetch(https);

        // then
        assertThat(overHttp).withFailMessage(() -> "an http URL was fetched").isEmpty();
        assertThat(loopbackOverHttps).withFailMessage(() -> "a host resolving to loopback was fetched").isEmpty();
        assertThat(requests.get()).withFailMessage(() -> "a request reached the server despite the policy").isZero();
    }

    @Test
    void keepsAtMostTheCapOfEntriesInItsCache() {
        // given: more distinct URLs than the cache holds, each a cached failure (the document names another client_id)
        var documents = documents();
        int urls = ClientIdMetadataDocuments.MAX_CACHED + 44;
        for (int i = 0; i < urls; i++) {
            documents.fetch(URI.create(url + "?n=" + i));
        }
        int afterFilling = requests.get();

        // when: the last URL (not kept) and the first (kept) are asked for again
        documents.fetch(URI.create(url + "?n=" + (urls - 1)));
        int afterLast = requests.get();
        documents.fetch(URI.create(url + "?n=0"));

        // then
        assertThat(afterLast).withFailMessage(() -> "an entry beyond the cap was cached").isEqualTo(afterFilling + 1);
        assertThat(requests.get()).withFailMessage(() -> "an entry within the cap was not cached").isEqualTo(afterLast);
    }

    private ClientIdMetadataDocuments documents() {
        return new ClientIdMetadataDocuments(clock, UrlPolicy.LOOPBACK_FOR_TESTS);
    }

    private static String document(String clientId, String rest) {
        return "{\"client_id\": \"" + clientId + "\", " + rest + "}";
    }

    /// A clock the test moves forward.
    private static final class MutableClock extends Clock {

        private Instant now = Instant.parse("2026-01-01T00:00:00Z");

        void advance(Duration duration) {
            now = now.plus(duration);
        }

        @Override
        public java.time.ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(java.time.ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }
    }
}

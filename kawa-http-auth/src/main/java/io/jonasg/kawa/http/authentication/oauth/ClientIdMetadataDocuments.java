package io.jonasg.kawa.http.authentication.oauth;

import com.nimbusds.jose.util.JSONObjectUtils;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/// Fetches the OAuth Client ID Metadata Documents (CIMD) of the clients whose `client_id` is a URL.
///
/// Any URL a client names may be asked for, so the [UrlPolicy] is the SSRF guard: the URL must be `https` and every
/// address its host resolves to must be public, or nothing is sent. Redirects are not followed, the request is
/// time-bounded and the body is capped at [#MAX_BODY_BYTES]. A document is accepted only when its `client_id` is the
/// URL it was fetched from, it lists at least one valid `redirect_uri` (an `http` loopback one without a port
/// allows any port), and its `token_endpoint_auth_method`, if present, is `none`: CIMD clients are public clients.
/// Anything else is logged at warn (URL and reason, never the body) and answered with empty.
///
/// Remaining risk (DNS rebinding): the address is checked here, but the HTTP client resolves the host again when it
/// connects, and kawa does not pin the checked address (that would break the TLS name check). A host whose DNS answers
/// differ between the two lookups can therefore still reach a non-public address; the response must also be a valid
/// document for its own URL and is never returned to the caller, which limits what it can leak.
///
/// Results are cached for [#TTL], failures for [#FAILURE_TTL], so a slow or broken host is not contacted on every
/// request; the cache holds at most [#MAX_CACHED] entries and an entry beyond that is simply not kept.
final class ClientIdMetadataDocuments {

    private static final Logger LOG = LoggerFactory.getLogger(ClientIdMetadataDocuments.class);

    private static final Pattern LOOPBACK_WITHOUT_PORT =
            Pattern.compile("(?i)(http://(?:localhost|127\\.0\\.0\\.1|\\[::1\\]))([/?].*)?");

    static final int MAX_BODY_BYTES = 64 * 1024;
    static final Duration TTL = Duration.ofMinutes(5);
    static final Duration FAILURE_TTL = Duration.ofSeconds(30);
    static final Duration TIMEOUT = Duration.ofSeconds(5);
    static final int MAX_CACHED = 256;
    static final int MAX_NAME_LENGTH = 100;

    /// What kawa needs from a client's document.
    ///
    /// @param redirectUris where the client may be redirected to
    /// @param clientName   the document's `client_name` for display: plain text, control characters removed and
    ///                     capped at [#MAX_NAME_LENGTH] characters; `null` when it has none
    record Document(@Nullable String clientName, List<RegisteredRedirectUri> redirectUris) {
    }

    private record Cached(Optional<Document> document, Instant expiry) {
    }

    private final HttpClient http;
    private final Duration timeout;
    private final Clock clock;
    private final UrlPolicy policy;
    private final Map<URI, Cached> cache = new ConcurrentHashMap<>();

    ClientIdMetadataDocuments(Clock clock, UrlPolicy policy) {
        this(clock, policy, TIMEOUT);
    }

    ClientIdMetadataDocuments(Clock clock, UrlPolicy policy, Duration timeout) {
        this.clock = clock;
        this.policy = policy;
        this.timeout = timeout;
        this.http = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NEVER)
                .connectTimeout(timeout)
                .build();
    }

    /// The document published at `url`, unless it could not be fetched or is not acceptable.
    Optional<Document> fetch(URI url) {
        var now = clock.instant();
        cache.values().removeIf(cached -> !cached.expiry().isAfter(now));
        var cached = cache.get(url);
        if (cached != null) {
            return cached.document();
        }
        var document = load(url);
        if (cache.size() < MAX_CACHED) {
            cache.put(url, new Cached(document, now.plus(document.isPresent() ? TTL : FAILURE_TTL)));
        }
        return document;
    }

    private Optional<Document> load(URI url) {
        var refusal = policy.refusal(url);
        if (refusal.isPresent()) {
            return rejected(url, refusal.get());
        }
        try {
            for (var address : InetAddress.getAllByName(url.getHost())) {
                if (!policy.permits(address)) {
                    return rejected(url, "resolves to an address kawa does not fetch from");
                }
            }
        } catch (UnknownHostException e) {
            return rejected(url, "has an unknown host");
        }
        try {
            var request = HttpRequest.newBuilder(url).timeout(timeout).header("Accept", "application/json").GET().build();
            var response = http.send(request, HttpResponse.BodyHandlers.ofInputStream());
            try (InputStream body = response.body()) {
                if (response.statusCode() != 200) {
                    return rejected(url, "answered with status " + response.statusCode());
                }
                var bytes = body.readNBytes(MAX_BODY_BYTES + 1);
                if (bytes.length > MAX_BODY_BYTES) {
                    return rejected(url, "is larger than " + MAX_BODY_BYTES + " bytes");
                }
                return parse(url, new String(bytes, StandardCharsets.UTF_8));
            }
        } catch (IOException e) {
            return rejected(url, "could not be fetched: " + e.getClass().getSimpleName());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return rejected(url, "fetch was interrupted");
        }
    }

    private static Optional<Document> parse(URI url, String body) {
        Map<String, Object> json;
        try {
            json = JSONObjectUtils.parse(body);
        } catch (ParseException e) {
            return rejected(url, "is not a JSON object");
        }
        if (!url.toString().equals(json.get("client_id"))) {
            return rejected(url, "has a client_id other than its URL");
        }
        var method = json.get("token_endpoint_auth_method");
        if (method != null && !"none".equals(method)) {
            return rejected(url, "asks for token_endpoint_auth_method " + method + ", but CIMD clients are public clients");
        }
        if (!(json.get("redirect_uris") instanceof List<?> values) || values.isEmpty()) {
            return rejected(url, "has no redirect_uris");
        }
        var redirectUris = new ArrayList<RegisteredRedirectUri>();
        for (var value : values) {
            var uri = value instanceof String string ? RegisteredRedirectUri.parse(anyLoopbackPort(string)) : Optional.<RegisteredRedirectUri>empty();
            if (uri.isEmpty() || uri.get().uri().getScheme() == null || uri.get().uri().getRawFragment() != null) {
                return rejected(url, "has an invalid redirect_uri");
            }
            redirectUris.add(uri.get());
        }
        return Optional.of(new Document(displayName(json.get("client_name")), List.copyOf(redirectUris)));
    }

    /// A client name safe to show: text only, without control, format (e.g. bidirectional override) or line
    /// separator characters, trimmed and capped; `null` when nothing is left.
    private static @Nullable String displayName(@Nullable Object value) {
        if (!(value instanceof String name)) {
            return null;
        }
        var clean = new StringBuilder();
        name.codePoints().filter(point -> {
            int type = Character.getType(point);
            return type != Character.CONTROL && type != Character.FORMAT
                    && type != Character.LINE_SEPARATOR && type != Character.PARAGRAPH_SEPARATOR;
        }).limit(MAX_NAME_LENGTH).forEach(clean::appendCodePoint);
        String result = clean.toString().strip();
        return result.isEmpty() ? null : result;
    }

    /// An `http` loopback redirect URI without a port allows any port, as Claude Code declares it (RFC 8252 §7.3).
    /// This applies to documents only; stored clients must write `:*`.
    private static String anyLoopbackPort(String redirectUri) {
        var matcher = LOOPBACK_WITHOUT_PORT.matcher(redirectUri);
        return matcher.matches() ? matcher.group(1) + ":*" + (matcher.group(2) == null ? "" : matcher.group(2)) : redirectUri;
    }

    private static Optional<Document> rejected(URI url, String reason) {
        LOG.warn("Client ID metadata document {} {}", url, reason);
        return Optional.empty();
    }
}

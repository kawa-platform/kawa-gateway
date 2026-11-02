package io.jonasg.kawa.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.jspecify.annotations.Nullable;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/// An OAuth client of kawa's built-in authorization server, such as an MCP client (Claude Code, Cursor, ...): a
/// public client using PKCE, without a secret. Not to be confused with [ClientConfig], the credentials of a Kafka
/// client. `kawa-studio` is configured separately and is not one of these.
///
/// @param id           the `client_id`: lower-case letters, digits and `-`, starting with a letter or digit, at
///                     most 64 characters, e.g. `kawa-mcp-claude`
/// @param name         an optional human-readable label
/// @param enabled      whether the client may sign in (defaults to `true`); switching it off also ends the
///                     sessions it already has
/// @param redirectUris the redirect URIs, at least one: absolute `https`, or `http` on `localhost`, `127.0.0.1`
///                     or `[::1]`, without a fragment. They are matched exactly, except that an `http` loopback
///                     URI may give `*` as its entire port, e.g. `http://localhost:*/callback`, for clients that
///                     listen on a random local port; the path is still matched exactly. `*` anywhere else is
///                     rejected. Stored as given
/// @param audience     where the client's tokens are accepted
public record OAuthClientConfig(
        String id,
        @Nullable String name,
        Boolean enabled,
        List<String> redirectUris,
        Audience audience
) {

    private static final Pattern ID = Pattern.compile("[a-z0-9][a-z0-9-]{0,63}");
    private static final Pattern LOOPBACK_PORT_WILDCARD =
            Pattern.compile("(?i)(http://(?:localhost|127\\.0\\.0\\.1|\\[::1])):\\*([/?].*)?");
    private static final Set<String> LOOPBACK_HOSTS = Set.of("127.0.0.1", "[::1]", "localhost");

    /// Where an OAuth client's tokens are accepted.
    public enum Audience {
        /// Only on `POST /mcp`.
        @JsonProperty("mcp") MCP,
        /// On the whole admin API, `/mcp` included.
        @JsonProperty("admin") ADMIN
    }

    public OAuthClientConfig {
        if (id == null || !ID.matcher(id).matches()) {
            throw new IllegalArgumentException("OAuth client id must match " + ID.pattern() + ": " + id);
        }
        if (audience == null) {
            throw new IllegalArgumentException("OAuth client '" + id + "' needs an audience");
        }
        enabled = enabled == null || enabled;
        redirectUris = redirectUris == null ? List.of() : List.copyOf(redirectUris);
        if (redirectUris.isEmpty()) {
            throw new IllegalArgumentException("OAuth client '" + id + "' needs at least one redirect URI");
        }
        redirectUris.forEach(uri -> validateRedirectUri(id, uri));
    }

    private static void validateRedirectUri(String id, @Nullable String value) {
        URI uri = parse(id, withoutLoopbackPortWildcard(id, value), value);
        if (uri.getRawFragment() != null) {
            throw invalidRedirectUri(id, value, "must not have a fragment");
        }
        if (!uri.isAbsolute() || uri.getHost() == null) {
            throw invalidRedirectUri(id, value, "must be an absolute URL");
        }
        boolean https = "https".equalsIgnoreCase(uri.getScheme());
        boolean loopbackHttp = "http".equalsIgnoreCase(uri.getScheme()) && isLoopbackHost(uri.getHost());
        if (!https && !loopbackHttp) {
            throw invalidRedirectUri(id, value, "must use https, or http on 127.0.0.1, [::1] or localhost");
        }
    }

    private static boolean isLoopbackHost(String host) {
        return LOOPBACK_HOSTS.contains(host.toLowerCase(Locale.ROOT));
    }

    /// `value` with a `:*` loopback port dropped, since [URI] cannot parse it; refuses `*` anywhere else.
    private static @Nullable String withoutLoopbackPortWildcard(String id, @Nullable String value) {
        if (value == null || !value.contains("*")) {
            return value;
        }
        Matcher matcher = LOOPBACK_PORT_WILDCARD.matcher(value);
        String rest = matcher.matches() ? Objects.requireNonNullElse(matcher.group(2), "") : "*";
        if (rest.contains("*")) {
            throw invalidRedirectUri(id, value, "may only use * as the entire port of an http URI on 127.0.0.1, [::1] or localhost");
        }
        return matcher.group(1) + rest;
    }

    private static URI parse(String id, @Nullable String value, @Nullable String original) {
        if (value == null) {
            throw invalidRedirectUri(id, null, "must not be null");
        }
        try {
            return new URI(value);
        } catch (URISyntaxException e) {
            throw invalidRedirectUri(id, original, "is not a valid URI");
        }
    }

    private static IllegalArgumentException invalidRedirectUri(String id, @Nullable String value, String reason) {
        return new IllegalArgumentException("redirect URI '" + value + "' of OAuth client '" + id + "' " + reason);
    }
}

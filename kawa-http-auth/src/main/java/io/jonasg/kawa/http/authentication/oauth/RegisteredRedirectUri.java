package io.jonasg.kawa.http.authentication.oauth;

import java.net.URI;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

/// A redirect URI an OAuth client registered, and which requested redirect URIs it allows.
///
/// A requested URI must match exactly, as [URI#equals] compares: scheme and host case-insensitively, everything
/// else (port, path, query) as written. The one exception is an `http` URI on `localhost`, `127.0.0.1` or `[::1]`
/// registered with `*` as its entire port, e.g. `http://localhost:*/callback`: it allows any port, with the same
/// scheme, host, path and query. There are no other wildcards and no prefix matching.
///
/// @param uri     the registered URI, without its port when `anyPort`
/// @param anyPort whether the URI was registered with `*` as its port
record RegisteredRedirectUri(URI uri, boolean anyPort) {

    private static final Pattern LOOPBACK_ANY_PORT =
            Pattern.compile("(?i)(http://(?:localhost|127\\.0\\.0\\.1|\\[::1])):\\*([/?].*)?");

    /// An exactly matching redirect URI.
    static RegisteredRedirectUri exact(URI uri) {
        return new RegisteredRedirectUri(uri, false);
    }

    /// The redirect URI as an OAuth client registered it, or empty when it is not a valid URI.
    static Optional<RegisteredRedirectUri> parse(String value) {
        var matcher = LOOPBACK_ANY_PORT.matcher(value);
        boolean anyPort = matcher.matches();
        String withoutPort = anyPort ? matcher.group(1) + Objects.requireNonNullElse(matcher.group(2), "") : value;
        try {
            return Optional.of(new RegisteredRedirectUri(new URI(withoutPort), anyPort));
        } catch (java.net.URISyntaxException e) {
            return Optional.empty();
        }
    }

    /// Whether a client may be redirected to `requested`.
    boolean allows(URI requested) {
        if (!anyPort) {
            return uri.equals(requested);
        }
        return requested.getScheme() != null && requested.getScheme().equalsIgnoreCase(uri.getScheme())
                && requested.getHost() != null && requested.getHost().equalsIgnoreCase(uri.getHost())
                && requested.getRawUserInfo() == null
                && Objects.equals(requested.getRawPath(), uri.getRawPath())
                && Objects.equals(requested.getRawQuery(), uri.getRawQuery())
                && requested.getRawFragment() == null;
    }
}

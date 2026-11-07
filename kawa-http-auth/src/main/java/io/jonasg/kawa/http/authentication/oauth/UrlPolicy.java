package io.jonasg.kawa.http.authentication.oauth;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/// Which URLs kawa may fetch on behalf of a client that names itself with a `client_id` URL (an SSRF guard).
///
/// [#PUBLIC_HTTPS] is the only policy production uses: `https` only, and every address the host resolves to must be
/// a public one ([#permits]): loopback, private, link-local, carrier-grade NAT, multicast, reserved and cloud
/// metadata addresses are refused, for IPv4, IPv6 and IPv4-mapped IPv6. [#LOOPBACK_FOR_TESTS] is the opposite and
/// exists so tests can serve a document from a local port; the provider only selects it through the system property
/// [#TEST_PROPERTY], never through `admin.auth.settings`.
///
/// A policy is checked in two steps: [#refusal] looks at the URL alone (no DNS) and is also applied to the URLs of the
/// `clientIdMetadataDocuments` setting at startup; [#permits] judges each address the host resolves to.
enum UrlPolicy {
    PUBLIC_HTTPS,
    LOOPBACK_FOR_TESTS;

    /// Setting this system property to `true` makes the `oauth` provider use [#LOOPBACK_FOR_TESTS]. For integration
    /// tests only: it is a JVM flag, not a gateway setting.
    static final String TEST_PROPERTY = "kawa.test.clientIdMetadataLoopback";

    private static final List<Cidr> BLOCKED = List.of(
            // IPv4: "this" network, private, CGNAT (also Alibaba's metadata 100.100.100.200), loopback, link-local
            // (also 169.254.169.254, the cloud metadata address), IETF/documentation/benchmarking, multicast, reserved
            Cidr.of("0.0.0.0/8"), Cidr.of("10.0.0.0/8"), Cidr.of("100.64.0.0/10"), Cidr.of("127.0.0.0/8"),
            Cidr.of("169.254.0.0/16"), Cidr.of("172.16.0.0/12"), Cidr.of("192.0.0.0/24"), Cidr.of("192.0.2.0/24"),
            Cidr.of("192.88.99.0/24"), Cidr.of("192.168.0.0/16"), Cidr.of("198.18.0.0/15"), Cidr.of("198.51.100.0/24"),
            Cidr.of("203.0.113.0/24"), Cidr.of("224.0.0.0/4"), Cidr.of("240.0.0.0/4"),
            // IPv6: unspecified, loopback and IPv4-compatible (::/96), NAT64, discard, documentation, 6to4, unique local
            // (also AWS's fd00:ec2::254), link-local, site-local, multicast
            Cidr.of("::/96"), Cidr.of("64:ff9b::/96"), Cidr.of("100::/64"), Cidr.of("2001:db8::/32"), Cidr.of("2002::/16"),
            Cidr.of("fc00::/7"), Cidr.of("fe80::/10"), Cidr.of("fec0::/10"), Cidr.of("ff00::/8"));

    /// Why `url` is refused before any address is looked up, or empty.
    Optional<String> refusal(URI url) {
        String scheme = url.getScheme();
        String host = url.getHost();
        String path = url.getRawPath();
        boolean schemeAllowed = "https".equalsIgnoreCase(scheme)
                || (this == LOOPBACK_FOR_TESTS && "http".equalsIgnoreCase(scheme));
        if (!schemeAllowed) {
            return Optional.of("must use https");
        }
        if (host == null || url.getRawUserInfo() != null || url.getRawFragment() != null) {
            return Optional.of("must have a host and no user info or fragment");
        }
        if (path == null || path.length() <= 1
                || Arrays.stream(path.split("/", -1)).skip(1).anyMatch(segment -> segment.equals(".") || segment.equals(".."))) {
            return Optional.of("must have a path without dot segments");
        }
        return Optional.empty();
    }

    /// Whether kawa may connect to `address` under this policy.
    boolean permits(InetAddress address) {
        return switch (this) {
            case LOOPBACK_FOR_TESTS -> address.isLoopbackAddress();
            case PUBLIC_HTTPS -> isPublic(address);
        };
    }

    private static boolean isPublic(InetAddress address) {
        byte[] bytes = address.getAddress();
        if (address instanceof Inet6Address && isIpv4Mapped(bytes)) {
            // ::ffff:a.b.c.d is judged as the IPv4 address it wraps
            bytes = Arrays.copyOfRange(bytes, 12, 16);
        }
        for (var cidr : BLOCKED) {
            if (cidr.contains(bytes)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isIpv4Mapped(byte[] bytes) {
        for (int i = 0; i < 10; i++) {
            if (bytes[i] != 0) {
                return false;
            }
        }
        return bytes[10] == (byte) 0xff && bytes[11] == (byte) 0xff;
    }

    private record Cidr(byte[] prefix, int bits) {

        static Cidr of(String cidr) {
            int slash = cidr.indexOf('/');
            return new Cidr(InetAddress.ofLiteral(cidr.substring(0, slash)).getAddress(), Integer.parseInt(cidr.substring(slash + 1)));
        }

        boolean contains(byte[] address) {
            if (address.length != prefix.length) {
                return false;
            }
            int whole = bits / 8;
            for (int i = 0; i < whole; i++) {
                if (address[i] != prefix[i]) {
                    return false;
                }
            }
            int rest = bits % 8;
            if (rest == 0) {
                return true;
            }
            int mask = 0xff << (8 - rest) & 0xff;
            return (address[whole] & mask) == (prefix[whole] & mask);
        }
    }
}

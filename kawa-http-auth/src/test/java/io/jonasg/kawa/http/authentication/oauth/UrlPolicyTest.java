package io.jonasg.kawa.http.authentication.oauth;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;

import static io.jonasg.kawa.test.KawaAssertions.assertThat;

/// The SSRF guard: which addresses and URLs kawa refuses to fetch a client's metadata document from.
class UrlPolicyTest {

    @ParameterizedTest(name = "{1}: {0}")
    @CsvSource({
            "127.0.0.1, loopback", "127.255.255.254, loopback",
            "10.0.0.1, private", "172.16.0.1, private", "172.31.255.255, private", "192.168.1.10, private",
            "169.254.169.254, link-local (cloud metadata)", "169.254.0.1, link-local",
            "100.64.0.1, CGNAT", "100.127.255.255, CGNAT", "100.100.100.200, CGNAT (Alibaba metadata)",
            "0.0.0.0, unspecified", "0.1.2.3, this network",
            "224.0.0.1, multicast", "239.255.255.255, multicast",
            "240.0.0.1, reserved", "255.255.255.255, broadcast",
            "192.0.2.1, documentation", "198.51.100.1, documentation", "203.0.113.1, documentation", "198.18.0.1, benchmarking",
            "::1, IPv6 loopback", "::, IPv6 unspecified",
            "fe80::1, IPv6 link-local", "fc00::1, IPv6 unique local", "fd00:ec2::254, AWS IPv6 metadata", "fec0::1, IPv6 site-local",
            "ff02::1, IPv6 multicast", "2001:db8::1, IPv6 documentation", "64:ff9b::7f00:1, NAT64", "2002:7f00:1::1, 6to4",
            "::ffff:127.0.0.1, mapped loopback", "::ffff:10.0.0.1, mapped private", "::ffff:169.254.169.254, mapped metadata",
    })
    void refusesNonPublicAddresses(String address, String kind) throws UnknownHostException {
        assertThat(UrlPolicy.PUBLIC_HTTPS.permits(InetAddress.getByName(address)))
                .withFailMessage(() -> kind + " address " + address + " was permitted")
                .isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = {"::ffff:7f00:1", "::ffff:a00:1", "::ffff:a9fe:a9fe"})
    void refusesIpv4MappedAddressesEvenWhenTheyStayIpv6(String address) throws UnknownHostException {
        // given: an Inet6Address that is not normalised to IPv4, as a resolver may return it
        var bytes = InetAddress.getByName(address).getAddress();
        var mapped = Inet6Address.getByAddress(null, mappedBytes(java.util.Arrays.copyOfRange(bytes, bytes.length - 4, bytes.length)), -1);

        // then
        assertThat(UrlPolicy.PUBLIC_HTTPS.permits(mapped)).withFailMessage(() -> "mapped address " + address + " was permitted").isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = {"8.8.8.8", "1.1.1.1", "93.184.216.34", "172.15.255.255", "172.32.0.1", "100.63.255.255", "100.128.0.1",
            "169.253.1.1", "2606:4700:4700::1111", "2a00:1450:4001:81b::200e", "::ffff:8.8.8.8"})
    void permitsPublicAddresses(String address) throws UnknownHostException {
        assertThat(UrlPolicy.PUBLIC_HTTPS.permits(InetAddress.getByName(address)))
                .withFailMessage(() -> "public address " + address + " was refused")
                .isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {"http://client.example.com/c.json", "ftp://client.example.com/c.json", "https://client.example.com",
            "https://client.example.com/", "https://client.example.com/c.json#frag", "https://user@client.example.com/c.json",
            "https://client.example.com/a/../c.json", "https://client.example.com/./c.json", "file:///etc/passwd"})
    void refusesUrlsThatAreNotHttpsOrHaveNoCleanPath(String url) {
        assertThat(UrlPolicy.PUBLIC_HTTPS.refusal(URI.create(url)))
                .withFailMessage(() -> "the URL " + url + " was accepted")
                .isPresent();
    }

    @ParameterizedTest
    @ValueSource(strings = {"https://client.example.com/c.json", "https://client.example.com:8443/oauth/client-metadata.json?v=1"})
    void acceptsAnHttpsUrlWithAPath(String url) {
        assertThat(UrlPolicy.PUBLIC_HTTPS.refusal(URI.create(url)))
                .withFailMessage(() -> "the URL " + url + " was refused")
                .isEmpty();
    }

    @org.junit.jupiter.api.Test
    void theTestPolicyPermitsLoopbackOverHttpAndNothingElse() throws UnknownHostException {
        assertThat(UrlPolicy.LOOPBACK_FOR_TESTS.refusal(URI.create("http://127.0.0.1:8080/c.json"))).isEmpty();
        assertThat(UrlPolicy.LOOPBACK_FOR_TESTS.permits(InetAddress.getByName("127.0.0.1"))).isTrue();
        assertThat(UrlPolicy.LOOPBACK_FOR_TESTS.permits(InetAddress.getByName("10.0.0.1"))).isFalse();
    }

    private static byte[] mappedBytes(byte[] ipv4) {
        var bytes = new byte[16];
        bytes[10] = (byte) 0xff;
        bytes[11] = (byte) 0xff;
        System.arraycopy(ipv4, 0, bytes, 12, 4);
        return bytes;
    }
}

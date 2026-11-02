package io.jonasg.kawa.http.authentication.oauth;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.net.URI;

import static io.jonasg.kawa.test.KawaAssertions.assertThat;

class RegisteredRedirectUriTest {

    @ParameterizedTest
    @CsvSource({
            "https://app.example.com/callback, https://app.example.com/callback",
            "http://localhost:8765/callback, http://localhost:8765/callback",
            "http://localhost:*/callback, http://localhost:8765/callback",
            "http://localhost:*/callback, http://localhost:1/callback",
            "http://127.0.0.1:*/callback, http://127.0.0.1:55123/callback",
            "http://[::1]:*/callback, http://[::1]:55123/callback",
            "HTTP://LocalHost:*/callback, http://localhost:4000/callback",
            "https://App.Example.com/callback, HTTPS://app.example.COM/callback",
            "http://localhost:*/callback?client=claude, http://localhost:4000/callback?client=claude",
    })
    void allows(String registered, String requested) {
        // given
        var redirectUri = RegisteredRedirectUri.parse(registered).orElseThrow();

        // when
        boolean allowed = redirectUri.allows(URI.create(requested));

        // then
        assertThat(allowed)
                .withFailMessage(() -> registered + " did not allow " + requested)
                .isTrue();
    }

    @ParameterizedTest
    @CsvSource({
            "https://app.example.com/callback, https://app.example.com/callback/more",
            "https://app.example.com/callback, https://app.example.com/Callback",
            "https://app.example.com/callback, https://evil.example.com/callback",
            "https://app.example.com/callback, http://app.example.com/callback",
            "https://app.example.com/callback, https://app.example.com:8443/callback",
            "http://localhost:8765/callback, http://localhost:8766/callback",
            "http://localhost:*/callback, http://localhost:4000/other",
            "http://localhost:*/callback, http://127.0.0.1:4000/callback",
            "http://localhost:*/callback, https://localhost:4000/callback",
            "http://localhost:*/callback, http://evil@localhost:4000/callback",
            "http://localhost:*/callback, http://localhost:4000/callback#fragment",
            "http://localhost:*/callback, http://localhost:4000/callback?client=claude",
            "http://localhost:*/callback?client=claude, http://localhost:4000/callback?client=other",
            "https://app.example.com/callback?a=1, https://app.example.com/callback?a=2",
    })
    void refuses(String registered, String requested) {
        // given
        var redirectUri = RegisteredRedirectUri.parse(registered).orElseThrow();

        // when
        boolean allowed = redirectUri.allows(URI.create(requested));

        // then
        assertThat(allowed)
                .withFailMessage(() -> registered + " allowed " + requested)
                .isFalse();
    }
}

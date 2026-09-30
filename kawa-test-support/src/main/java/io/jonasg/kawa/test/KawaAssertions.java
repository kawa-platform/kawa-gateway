package io.jonasg.kawa.test;

import org.assertj.core.api.Assertions;

import java.net.http.HttpResponse;

/// Entry point for AssertJ plus kawa's own assertions: statically import `assertThat` from here
/// instead of from [Assertions] to get [HttpResponseAssert] for HTTP responses.
public final class KawaAssertions extends Assertions {

    private KawaAssertions() {
    }

    public static HttpResponseAssert assertThat(HttpResponse<String> response) {
        return new HttpResponseAssert(response);
    }
}

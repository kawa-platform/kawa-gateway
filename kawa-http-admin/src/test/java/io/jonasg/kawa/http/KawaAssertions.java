package io.jonasg.kawa.http;

import org.assertj.core.api.Assertions;

import java.net.http.HttpResponse;

public final class KawaAssertions extends Assertions {

    private KawaAssertions() {
    }

    public static HttpResponseAssert assertThat(HttpResponse<String> response) {
        return new HttpResponseAssert(response);
    }
}

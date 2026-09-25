package io.jonasg.kawa.config;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;

class BrokerAuthConfigTest {

    @Test
    void iamDoesNotRequireUsernameOrPassword() {
        // given
        // when / then
        assertThatCode(() -> new BrokerAuthConfig("AWS_MSK_IAM", null, null))
                .doesNotThrowAnyException();
    }
}

package io.jonasg.kawa.server.auth;

import io.jonasg.kawa.config.BrokerAuthConfig;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class BrokerSaslMechanismTest {

    @Test
    void resolvesPlainMechanism() {
        // given
        var config = new BrokerAuthConfig("PLAIN", "gateway", "secret");

        // when
        var mechanism = BrokerSaslMechanism.from(config);

        // then
        assertThat(mechanism.name()).isEqualTo("PLAIN");
        assertThat(mechanism.authBytes("broker.example"))
                .containsExactly(0, 'g', 'a', 't', 'e', 'w', 'a', 'y', 0, 's', 'e', 'c', 'r', 'e', 't');
    }

    @Test
    void resolvesIamMechanism() {
        // given
        var config = new BrokerAuthConfig("AWS_MSK_IAM", null, null, "us-east-1", null);

        // when
        var mechanism = BrokerSaslMechanism.from(config);

        // then
        assertThat(mechanism.name()).isEqualTo("AWS_MSK_IAM");
    }
}

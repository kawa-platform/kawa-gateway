package io.jonasg.kawa.config;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BrokerAuthMechanismTest {

    @Test
    void buildsIamKafkaClientProperties() {
        // given
        var config = new BrokerAuthConfig("AWS_MSK_IAM", null, null, "us-east-1", "developer");

        // when
        var properties = BrokerAuthMechanisms.resolve(config.mechanism()).kafkaClientProperties(config);

        // then
        assertThat(properties.getProperty("security.protocol")).isEqualTo("SASL_SSL");
        assertThat(properties.getProperty("sasl.mechanism")).isEqualTo("AWS_MSK_IAM");
        assertThat(properties.getProperty("sasl.client.callback.handler.class"))
                .isEqualTo("software.amazon.msk.auth.iam.IAMClientCallbackHandler");
        assertThat(properties.getProperty("sasl.jaas.config"))
                .isEqualTo("software.amazon.msk.auth.iam.IAMLoginModule required "
                        + "awsRegion=\"us-east-1\" awsProfileName=\"developer\";");
    }

    @Test
    void buildsPlainKafkaClientProperties() {
        // given
        var config = new BrokerAuthConfig("PLAIN", "gateway", "secret");

        // when
        var properties = BrokerAuthMechanisms.resolve(config.mechanism()).kafkaClientProperties(config);

        // then
        assertThat(properties.getProperty("security.protocol")).isEqualTo("SASL_PLAINTEXT");
        assertThat(properties.getProperty("sasl.mechanism")).isEqualTo("PLAIN");
        assertThat(properties.getProperty("sasl.jaas.config"))
                .isEqualTo("org.apache.kafka.common.security.plain.PlainLoginModule required "
                        + "username=\"gateway\" password=\"secret\";");
    }

    @Test
    void rejectsUnknownMechanism() {
        // given
        // when / then
        assertThatThrownBy(() -> BrokerAuthMechanisms.resolve("SCRAM-SHA-256"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("SCRAM-SHA-256");
    }
}

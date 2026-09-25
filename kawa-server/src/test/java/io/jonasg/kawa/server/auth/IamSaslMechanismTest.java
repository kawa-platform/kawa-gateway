package io.jonasg.kawa.server.auth;

import io.jonasg.kawa.config.BrokerAuthConfig;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import static org.assertj.core.api.Assertions.assertThat;

class IamSaslMechanismTest {

    private static final String BROKER_HOST = "b-1.cluster.abc.c2.kafka.us-east-1.amazonaws.com";

    @Test
    void generatesProvisionedMskPayloadWithFixedCredentials() {
        // given
        var config = new BrokerAuthConfig("AWS_MSK_IAM", null, null, "us-east-1", null);
        AwsCredentialsProvider credentials = () -> AwsBasicCredentials.create("ACCESS", "SECRET");
        var mechanism = new IamSaslMechanism(config, credentials);

        // when
        String payload = new String(mechanism.authBytes(BROKER_HOST));

        // then
        assertThat(payload).contains("\"host\":\"" + BROKER_HOST + "\"");
        assertThat(payload).contains("\"action\":\"kafka-cluster:Connect\"");
        assertThat(payload).contains("ACCESS/202");
        assertThat(payload).contains("\"x-amz-signature\"");
    }
}

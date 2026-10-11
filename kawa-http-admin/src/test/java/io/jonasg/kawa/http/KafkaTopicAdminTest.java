package io.jonasg.kawa.http;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaTopicAdminTest {

    @Test
    void addsTheBootstrapServersToTheBrokerSecuritySettings() {
        // given
        var security = new Properties();
        security.put("security.protocol", "SASL_SSL");
        security.put("ssl.truststore.location", "/etc/kawa/truststore.p12");

        // when
        var props = KafkaTopicAdmin.props("localhost:9092", security);

        // then
        assertThat(props)
                .containsEntry("bootstrap.servers", "localhost:9092")
                .containsEntry("security.protocol", "SASL_SSL")
                .containsEntry("ssl.truststore.location", "/etc/kawa/truststore.p12");
    }
}

package io.jonasg.kawa.server.broker;

import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class BrokerClientTimeoutTest {

    @Test
    void waitsForAJoinGroupAsLongAsTheBrokerMayHoldItForTheRebalance() {
        // given - the broker holds a JoinGroup until every member rejoined, up to the rebalance timeout
        var join = new JoinGroupRequestData().setRebalanceTimeoutMs(300_000).setSessionTimeoutMs(45_000);

        // when
        long timeout = BrokerClient.timeoutMillis(join);

        // then
        assertThat(timeout)
                .withFailMessage(() -> "A JoinGroup with a 5 minute rebalance timeout would be cut off after " + timeout + " ms")
                .isEqualTo(305_000);
    }

    @Test
    void boundsAJoinGroupWithoutARebalanceTimeoutBySessionTimeout() {
        // given - JoinGroup v0 carries no rebalance timeout
        var join = new JoinGroupRequestData().setRebalanceTimeoutMs(-1).setSessionTimeoutMs(45_000);

        // when / then
        assertThat(BrokerClient.timeoutMillis(join)).isEqualTo(50_000);
    }

    @Test
    void waitsForAFetchAtLeastItsMaxWait() {
        // given
        var fetch = new FetchRequestData().setMaxWaitMs(60_000);

        // when / then
        assertThat(BrokerClient.timeoutMillis(fetch)).isEqualTo(65_000);
    }

    @Test
    void usesTheDefaultTimeoutForEveryOtherRequest() {
        // when / then
        assertThat(BrokerClient.timeoutMillis(new ProduceRequestData()))
                .withFailMessage(() -> "Requests the broker answers right away should keep the 30 s default")
                .isEqualTo(30_000);
        assertThat(BrokerClient.timeoutMillis(new FetchRequestData().setMaxWaitMs(500))).isEqualTo(30_000);
        assertThat(BrokerClient.timeoutMillis(null)).isEqualTo(30_000);
    }
}

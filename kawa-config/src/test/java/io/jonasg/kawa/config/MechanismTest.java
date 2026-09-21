package io.jonasg.kawa.config;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MechanismTest {

    @Test
    void resolvesWireNames() {
        // given / when / then
        assertThat(Mechanism.fromWireName("PLAIN")).isEqualTo(Mechanism.PLAIN);
        assertThat(Mechanism.fromWireName("SCRAM-SHA-256")).isEqualTo(Mechanism.SCRAM_SHA_256);
    }

    @Test
    void exposesWireName() {
        // given / when / then
        assertThat(Mechanism.PLAIN.wireName()).isEqualTo("PLAIN");
        assertThat(Mechanism.SCRAM_SHA_256.wireName()).isEqualTo("SCRAM-SHA-256");
    }

    @Test
    void rejectsUnknownWireName() {
        assertThatThrownBy(() -> Mechanism.fromWireName("SCRAM-SHA256"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown mechanism");
    }
}

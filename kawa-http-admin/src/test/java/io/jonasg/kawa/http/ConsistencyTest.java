package io.jonasg.kawa.http;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConsistencyTest {

    @Test
    void parsesNullAsPersisted() {
        assertThat(Consistency.fromQueryParam(null)).isEqualTo(Consistency.PERSISTED);
    }

    @Test
    void parsesPersistedAsPersisted() {
        assertThat(Consistency.fromQueryParam("persisted")).isEqualTo(Consistency.PERSISTED);
    }

    @Test
    void parsesAppliedAsApplied() {
        assertThat(Consistency.fromQueryParam("applied")).isEqualTo(Consistency.APPLIED);
    }

    @Test
    void rejectsUnknownValue() {
        assertThatThrownBy(() -> Consistency.fromQueryParam("strong"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("invalid consistency 'strong'")
                .hasMessageContaining("expected 'persisted' or 'applied'");
    }
}

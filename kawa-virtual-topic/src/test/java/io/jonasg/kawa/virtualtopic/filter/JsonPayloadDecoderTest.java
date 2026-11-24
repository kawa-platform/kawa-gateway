package io.jonasg.kawa.virtualtopic.filter;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JsonPayloadDecoderTest {

    @Test
    void leavesTheValueOutOfAParseError() {
        // given
        var payload = ByteBuffer.wrap("{\"card\": secret4111}".getBytes(StandardCharsets.UTF_8));

        // then
        assertThatThrownBy(() -> new JsonPayloadDecoder().decode(payload))
                .isInstanceOf(PayloadDecodeException.class)
                .hasMessageContaining("not valid JSON at line 1, column")
                .hasMessageNotContaining("secret");
    }
}

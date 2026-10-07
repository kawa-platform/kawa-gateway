package io.jonasg.kawa.config;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AdminUserConfigTest {

    private static final HashedPassword PASSWORD = HashedPassword.fromPlaintext(Mechanism.PLAIN, "s3cret");
    private static final Instant CREATED_AT = Instant.parse("2026-01-01T00:00:00Z");

    @Test
    void storesTheEmailInLowerCase() {
        // when
        var user = new AdminUserConfig(" Alice@MyCom.com ", PASSWORD, null, true, CREATED_AT);

        // then
        assertThat(user.email())
                .withFailMessage(() -> "the email was not normalized: " + user.email())
                .isEqualTo("alice@mycom.com");
    }

    @Test
    void rejectsAnInvalidEmail() {
        // when / then
        assertThatThrownBy(() -> new AdminUserConfig("alice", PASSWORD, null, true, CREATED_AT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("'alice' is not an email address");
    }

    @Test
    void rejectsNullEmail() {
        // when / then
        assertThatThrownBy(() -> new AdminUserConfig(null, PASSWORD, null, true, CREATED_AT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("email must not be null");
    }

    @Test
    void rejectsNullPassword() {
        // when / then
        assertThatThrownBy(() -> new AdminUserConfig("alice@mycom.com", null, "Alice", true, CREATED_AT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("password must not be null");
    }

    @Test
    void rejectsNullCreatedAt() {
        // when / then
        assertThatThrownBy(() -> new AdminUserConfig("alice@mycom.com", PASSWORD, "Alice", true, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("createdAt must not be null");
    }
}

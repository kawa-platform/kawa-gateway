package io.jonasg.kawa.http;

import io.jonasg.kawa.config.AdminUserConfig;
import io.jonasg.kawa.config.HashedPassword;
import io.jonasg.kawa.config.Mechanism;
import org.jspecify.annotations.Nullable;

import java.time.Instant;

/// Object Mother for [AdminUserConfig]: `adminUser().build()` returns an enabled admin user with
/// sensible defaults; override only what a test cares about via the `withX` methods.
final class AdminUserConfigMother {

    static final String ALICE_ID = "3f2c1a9e-5b7d-4c1e-9a2b-6d8e0f1a2b3c";
    static final String BOB_ID = "7a9b8c6d-1e2f-4a3b-8c4d-5e6f7a8b9c0d";
    static final Instant CREATED_AT = Instant.parse("2026-01-01T00:00:00Z");

    private AdminUserConfigMother() {
    }

    static Builder adminUser() {
        return new Builder();
    }

    static final class Builder {

        private String email = "alice@mycom.com";
        private String password = "alice-secret";
        private @Nullable String displayName;
        private boolean enabled = true;
        private Instant createdAt = CREATED_AT;

        private Builder() {
        }

        Builder withEmail(String email) {
            this.email = email;
            return this;
        }

        Builder withPassword(String password) {
            this.password = password;
            return this;
        }

        Builder withDisplayName(String displayName) {
            this.displayName = displayName;
            return this;
        }

        Builder disabled() {
            this.enabled = false;
            return this;
        }

        Builder withCreatedAt(Instant createdAt) {
            this.createdAt = createdAt;
            return this;
        }

        AdminUserConfig build() {
            return new AdminUserConfig(email, HashedPassword.fromPlaintext(Mechanism.PLAIN, password), displayName, enabled, createdAt);
        }
    }
}

package io.jonasg.kawa.config;

/// An admin API user stored in the config topic. The password is kept as a PBKDF2-HMAC-SHA256
/// hash (the PLAIN encoding of [HashedPassword]); plaintext never enters the config model.
///
/// @param password the hashed password
public record AdminUserConfig(HashedPassword password) {

    public AdminUserConfig {
        if (password == null) {
            throw new IllegalArgumentException("password must not be null");
        }
    }

    /// Hashes a plaintext password into an admin user.
    public static AdminUserConfig fromPlaintext(String plaintext) {
        return new AdminUserConfig(HashedPassword.fromPlaintext(Mechanism.PLAIN, plaintext));
    }
}

package io.jonasg.kawa.http.users;

import io.jonasg.kawa.config.AdminAuthConfig;
import io.jonasg.kawa.config.AdminUserConfig;
import io.jonasg.kawa.config.DynamicConfigRepository;
import io.jonasg.kawa.http.configchange.ConflictException;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/// Setup mode: the first start of a gateway with `admin.auth.type: oauth`, without
/// `admin.auth.bootstrapUser` and without admin users. Nobody can sign in yet, so `POST /admin/setup`
/// lets kawa studio create the first admin user, once, with a setup token.
///
/// The token is `KAWA_SETUP_TOKEN` when set, so replicas can share one (e.g. from a Kubernetes secret);
/// otherwise each instance generates its own at startup (256 random bits, base64url) and logs it once at
/// WARN. A token from the environment is never logged. Tokens are compared in constant time, never
/// echoed, and invalidated once the first admin user exists; their entropy is the guard against
/// guessing, there is no rate limit.
///
/// Whether setup mode applies is decided once, at startup after the config topic has been read and the
/// bootstrap user seeded. Setup is required from then on only while no admin user exists, read from the
/// shared config on every request, so after any replica created the first admin user every replica
/// refuses setup. The creation itself is checked on the snapshot that is written
/// ([AdminUserService#createFirstAdminUser]), so concurrent setups on several replicas store exactly one.
public final class AdminSetup {

    /// The environment variable that supplies the setup token instead of a generated one.
    public static final String TOKEN_ENV = "KAWA_SETUP_TOKEN";

    static final String NOT_REQUIRED = "setup is not required: the gateway is not in setup mode or already has an admin user";

    private static final Logger log = LoggerFactory.getLogger(AdminSetup.class);
    private static final int TOKEN_BYTES = 32;
    private static final String RULE = "=".repeat(72);

    private final DynamicConfigRepository repository;
    private final AdminUserService adminUsers;
    /// The setup token, `null` when setup is not (or no longer) possible on this instance.
    private final AtomicReference<byte @Nullable []> token;

    private AdminSetup(DynamicConfigRepository repository, AdminUserService adminUsers, byte @Nullable [] token) {
        this.repository = repository;
        this.adminUsers = adminUsers;
        this.token = new AtomicReference<>(token);
    }

    /// Enters setup mode when `auth` and the stored admin users call for it, and logs how to complete it.
    /// Call after the config topic has been read and the bootstrap user seeded.
    ///
    /// @param env looks up [#TOKEN_ENV]
    public static AdminSetup start(AdminAuthConfig auth, DynamicConfigRepository repository, AdminUserService adminUsers,
                                   Function<String, @Nullable String> env) {
        boolean setupMode = AdminAuthConfig.OAUTH_TYPE.equals(auth.type())
                && auth.bootstrapUser() == null
                && repository.getActiveConfigOrEmpty().adminUsers().isEmpty();
        if (!setupMode) {
            return new AdminSetup(repository, adminUsers, null);
        }
        String studio = auth.settings().get("loginUrl") instanceof String loginUrl && !loginUrl.isBlank()
                ? "Open kawa studio at " + loginUrl
                : "Open kawa studio";
        String fromEnv = env.apply(TOKEN_ENV);
        if (fromEnv != null && !fromEnv.isBlank()) {
            log.warn(banner(studio,
                    "  2. Enter the setup token from the " + TOKEN_ENV + " environment variable.",
                    "  It works until the first admin user exists."));
            return new AdminSetup(repository, adminUsers, fromEnv.trim().getBytes(StandardCharsets.UTF_8));
        }
        String generated = generateToken();
        log.warn(banner(studio,
                "  2. Enter this setup token:\n\n       " + generated,
                "  It works until the first admin user exists and changes on every restart.\n"
                        + "  Set " + TOKEN_ENV + " to give every gateway instance the same token."));
        return new AdminSetup(repository, adminUsers, generated.getBytes(StandardCharsets.UTF_8));
    }

    /// The setup-mode announcement: a framed block that stands out in the startup log.
    private static String banner(String studio, String tokenStep, String tokenNote) {
        return "\n" + RULE + "\n"
                + "  kawa setup: create the first admin user\n\n"
                + "  1. " + studio + "\n"
                + tokenStep + "\n\n"
                + tokenNote + "\n"
                + "  To skip setup, set admin.auth.bootstrapUser and restart.\n"
                + RULE;
    }

    /// Whether the first admin user still has to be created through setup.
    public boolean required() {
        if (token.get() == null) {
            return false;
        }
        if (!repository.getActiveConfigOrEmpty().adminUsers().isEmpty()) {
            // another replica completed setup: this token is done for good
            token.set(null);
            return false;
        }
        return true;
    }

    /// Whether `presented` is the setup token. `false` once setup is done.
    boolean acceptsToken(@Nullable String presented) {
        byte[] expected = token.get();
        return expected != null && presented != null
                && MessageDigest.isEqual(expected, presented.getBytes(StandardCharsets.UTF_8));
    }

    /// Stores `user` as the first admin user and ends setup; returns its id.
    ///
    /// @throws InvalidSetupTokenException when `presented` is not the setup token
    /// @throws ConflictException          when setup is not required (any longer)
    String createFirstAdminUser(@Nullable String presented, AdminUserConfig user) {
        if (!required()) {
            throw new ConflictException(NOT_REQUIRED);
        }
        if (!acceptsToken(presented)) {
            throw new InvalidSetupTokenException();
        }
        String id = adminUsers.createFirstAdminUser(user);
        token.set(null);
        log.info("created the first admin user '{}' through setup; setup mode has ended", user.email());
        return id;
    }

    private static String generateToken() {
        byte[] bytes = new byte[TOKEN_BYTES];
        new SecureRandom().nextBytes(bytes);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }

    /// The presented setup token is missing or wrong.
    static final class InvalidSetupTokenException extends RuntimeException {

        InvalidSetupTokenException() {
            super("invalid setup token");
        }
    }
}

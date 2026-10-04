package io.jonasg.kawa.plugin.auth;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

/// The `admin.auth.settings` section handed to the selected authentication provider. `${VAR}` and
/// `${VAR:-default}` placeholders in string values are already resolved from the environment.
public interface ProviderSettings {

    /// The string setting `key`, if present and not blank.
    Optional<String> string(String key);

    /// The string setting `key`; throws [IllegalArgumentException] naming the setting when it is absent.
    String requireString(String key);

    /// The ISO-8601 duration setting `key` (e.g. `PT15M`), if present; throws
    /// [IllegalArgumentException] naming the setting when it is not a valid duration.
    Optional<Duration> duration(String key);

    /// The list setting `key`: a YAML list or a comma-separated string; empty when absent.
    List<String> strings(String key);
}

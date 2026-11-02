package io.jonasg.kawa.plugin.auth;

import java.util.Optional;

/// The OAuth clients managed through kawa's admin API (`/admin/oauth-clients`) and stored in the config topic.
///
/// Every call reads the current state, so a client added, changed, disabled or deleted through the API is seen on
/// the next call. Providers must look clients up when they need them and not cache the result, or disabling a
/// client would not take effect immediately.
public interface OAuthClientDirectory {

    /// The client with this `id`, disabled ones included, or empty when there is none.
    Optional<OAuthClient> find(String id);
}

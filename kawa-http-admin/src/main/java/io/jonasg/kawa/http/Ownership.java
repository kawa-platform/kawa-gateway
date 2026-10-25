package io.jonasg.kawa.http;

import io.jonasg.kawa.config.ManagedBy;
import io.jonasg.kawa.plugin.auth.KawaPrincipal;
import org.jspecify.annotations.Nullable;

/// Ownership of managed config entries (virtual topics, roles, groups, clients).
///
/// The gateway stamps the owner itself; a request body never decides it. A write by the kawa
/// operator, acting as itself or for another principal, marks the entry `managedBy: kawa-operator`;
/// a write by anyone else leaves an unmanaged entry unmanaged. Only the owner may change, delete or
/// release a managed entry; anyone else gets a [ConflictException] (`409`).
///
/// Services call these checks inside the repository update lambda, so they run against the same
/// snapshot the write commits on and are re-evaluated whenever the write retries.
final class Ownership {

    /// The operator's OAuth client id, `TokenIssuer.OPERATOR_CLIENT_ID` in `kawa-http-auth`,
    /// which this module must not depend on.
    static final String OPERATOR = "kawa-operator";

    private Ownership() {
    }

    /// The marker an entry carries after `caller` writes it: the operator's for an operator write,
    /// `null` (unmanaged) for anyone else. Only meaningful once [#requireOwner] admitted the write.
    static @Nullable ManagedBy stamp(@Nullable KawaPrincipal caller) {
        return isOperator(caller) ? new ManagedBy(OPERATOR) : null;
    }

    /// Refuses the write with `409` unless the entry is unmanaged or `caller` is its owner.
    static void requireOwner(String kind, String name, @Nullable ManagedBy current, @Nullable KawaPrincipal caller) {
        if (current != null && !current.principal().equals(identity(caller))) {
            throw new ConflictException(kind + " '" + name + "' is managed by " + current.principal()
                                        + "; change it through Kubernetes");
        }
    }

    /// The owner name a caller writes under, or `null` when it owns nothing. Only the operator is
    /// ever stamped as an owner, so every other caller, even one named `kawa-operator`, owns nothing.
    private static @Nullable String identity(@Nullable KawaPrincipal caller) {
        return isOperator(caller) ? OPERATOR : null;
    }

    /// Whether the operator made the request. Only the actor proves it: the authentication provider
    /// sets it for a verified operator token alone, while any admin user may be named `kawa-operator`.
    private static boolean isOperator(@Nullable KawaPrincipal caller) {
        return caller != null && OPERATOR.equals(caller.actor());
    }
}

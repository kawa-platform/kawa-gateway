package io.jonasg.kawa.http.configchange;

/// Thrown by a service when an operation is rejected by policy, so the handler can
/// translate it into a `403 Forbidden` response.
public final class ForbiddenException extends RuntimeException {

    public ForbiddenException(String message) {
        super(message);
    }
}

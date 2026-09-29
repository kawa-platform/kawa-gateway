package io.jonasg.kawa.http;

/// Thrown by a service when an operation cannot complete because of a conflicting
/// resource, so the handler can translate it into a `409 Conflict` response.
public final class ConflictException extends RuntimeException {

    public ConflictException(String message) {
        super(message);
    }
}

package io.jonasg.kawa.http;

/// Thrown by a service when a referenced resource does not exist, so the handler can
/// translate it into a `404 Not Found` response.
public final class NotFoundException extends RuntimeException {

    public NotFoundException(String message) {
        super(message);
    }
}

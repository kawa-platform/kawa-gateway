package io.jonasg.kawa.virtualtopic.filter;

/// A value tree could not be encoded in the target format, see [PayloadEncoder].
public class PayloadEncodeException extends RuntimeException {

    public PayloadEncodeException(String message) {
        super(message);
    }

    public PayloadEncodeException(String message, Throwable cause) {
        super(message, cause);
    }
}

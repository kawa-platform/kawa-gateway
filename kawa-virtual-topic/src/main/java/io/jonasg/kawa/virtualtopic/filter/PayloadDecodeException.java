package io.jonasg.kawa.virtualtopic.filter;

/// A record payload could not be decoded in its virtual topic's configured format. Handled per
/// the format's [io.jonasg.kawa.config.DecodeErrorPolicy].
public class PayloadDecodeException extends RuntimeException {

    public PayloadDecodeException(String message) {
        super(message);
    }

    public PayloadDecodeException(String message, Throwable cause) {
        super(message, cause);
    }
}

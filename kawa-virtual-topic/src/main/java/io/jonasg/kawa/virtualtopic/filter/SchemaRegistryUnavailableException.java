package io.jonasg.kawa.virtualtopic.filter;

/// The schema registry could not be asked: it is unreachable, did not answer in time, or answered
/// with an HTTP 5xx error.
///
/// This is an outage, not a record error, so it is never handled per a format's `onDecodeError`
/// policy: skipping or including records while the registry is down would let consumers commit
/// past records they never got. It fails the fetched partition instead, so the consumer fetches
/// it again once the registry is back.
public class SchemaRegistryUnavailableException extends RuntimeException {

    public SchemaRegistryUnavailableException(String message) {
        super(message);
    }

    public SchemaRegistryUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}

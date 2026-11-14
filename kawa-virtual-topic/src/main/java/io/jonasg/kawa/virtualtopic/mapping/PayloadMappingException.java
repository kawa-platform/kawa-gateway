package io.jonasg.kawa.virtualtopic.mapping;

/// A record value could not be mapped by a virtual topic's transformations, for example because
/// a path runs into a scalar where it needs an object, or a `cel` step failed for this record.
public class PayloadMappingException extends RuntimeException {

    public PayloadMappingException(String message) {
        super(message);
    }

    public PayloadMappingException(String message, Throwable cause) {
        super(message, cause);
    }
}

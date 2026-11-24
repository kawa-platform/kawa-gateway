package io.jonasg.kawa.virtualtopic.mapping;

/// A record value could not be mapped by a virtual topic's transformations, for example because
/// a path runs into a scalar where it needs an object, or a `cel` step failed for this record.
public class PayloadMappingException extends RuntimeException {

    /// [#step()] when the failing step is not known.
    public static final int UNKNOWN_STEP = -1;

    private final int step;

    public PayloadMappingException(String message) {
        super(message);
        this.step = UNKNOWN_STEP;
    }

    public PayloadMappingException(String message, Throwable cause) {
        super(message, cause);
        this.step = UNKNOWN_STEP;
    }

    /// @param step the 0-based index, in the virtual topic's `transformations`, of the failing step
    public PayloadMappingException(String message, Throwable cause, int step) {
        super(message, cause);
        this.step = step;
    }

    /// The 0-based index, in the virtual topic's `transformations`, of the step that failed, or
    /// [#UNKNOWN_STEP].
    public int step() {
        return step;
    }
}

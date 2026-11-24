package io.jonasg.kawa.virtualtopic.filter;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.jspecify.annotations.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/// The headers that tell a consumer why a record reached it unfiltered or untransformed: under
/// the `include` policy, a record the filter or a transformation fails on is delivered anyway, and
/// carries
///
/// | Header             | Value                                                                   |
/// |--------------------|-------------------------------------------------------------------------|
/// | `kawa-error`       | the failure: its exception type and message, cut at [#MAX_REASON_LENGTH] |
/// | `kawa-error-stage` | `filter` or `transformation`                                            |
/// | `kawa-error-step`  | the 0-based index of the failing transformation step; transformations only |
///
/// The reason never holds a stack trace, and never the payload: failure messages name kinds, paths
/// and locations, not values.
public final class ErrorHeaders {

    public static final String ERROR = "kawa-error";
    public static final String STAGE = "kawa-error-stage";
    public static final String STEP = "kawa-error-step";
    public static final String FILTER_STAGE = "filter";
    public static final String TRANSFORMATION_STAGE = "transformation";
    /// The longest reason, in characters; a longer one is cut.
    public static final int MAX_REASON_LENGTH = 256;

    private ErrorHeaders() {
    }

    /// `headers` with the `kawa-error` headers for `failure`: every other header is kept in order,
    /// and any `kawa-error*` header the record already had is replaced, not repeated.
    ///
    /// @param step the index of the failing transformation step, `null` for the filter stage
    public static Header[] with(Header[] headers, RuntimeException failure, String stage, @Nullable Integer step) {
        List<Header> result = new ArrayList<>(headers.length + 3);
        for (Header header : headers) {
            if (!isErrorHeader(header.key())) {
                result.add(header);
            }
        }
        result.add(header(ERROR, reason(failure)));
        result.add(header(STAGE, stage));
        if (step != null) {
            result.add(header(STEP, step.toString()));
        }
        return result.toArray(Header[]::new);
    }

    static String reason(RuntimeException failure) {
        String reason = failure.getClass().getSimpleName() + ": " + failure.getMessage();
        if (reason.length() <= MAX_REASON_LENGTH) {
            return reason;
        }
        // never split a surrogate pair, which would not encode as UTF-8
        int end = Character.isHighSurrogate(reason.charAt(MAX_REASON_LENGTH - 1)) ? MAX_REASON_LENGTH - 1 : MAX_REASON_LENGTH;
        return reason.substring(0, end);
    }

    private static boolean isErrorHeader(String key) {
        return key.equals(ERROR) || key.equals(STAGE) || key.equals(STEP);
    }

    private static Header header(String key, String value) {
        return new RecordHeader(key, value.getBytes(StandardCharsets.UTF_8));
    }
}

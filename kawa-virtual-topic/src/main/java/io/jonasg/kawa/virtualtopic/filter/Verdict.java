package io.jonasg.kawa.virtualtopic.filter;

import org.apache.kafka.common.header.Header;
import org.jspecify.annotations.Nullable;

/// What a virtual topic's consume filter decided about one record: drop it, or keep it. A record
/// the filter could not decode but keeps under the `include` policy is kept with its `failure`.
///
/// @param keep        whether the record is delivered
/// @param failure     why the filter could not evaluate the record it keeps, `null` when it could
/// @param errorHeader whether that failure is written to the record as [ErrorHeaders]
public record Verdict(boolean keep, @Nullable PayloadDecodeException failure, boolean errorHeader) {

    public static final Verdict DROP = new Verdict(false, null, false);
    public static final Verdict KEEP = new Verdict(true, null, false);

    static Verdict keptDespite(PayloadDecodeException failure, boolean errorHeader) {
        return new Verdict(true, failure, errorHeader);
    }

    /// Whether the record is delivered with the [ErrorHeaders] of a failure.
    public boolean flagged() {
        return failure != null && errorHeader;
    }

    /// The headers to deliver the record with: `headers`, plus the [ErrorHeaders] of the failure
    /// when it is [#flagged()].
    public Header[] headers(Header[] headers) {
        return flagged() ? ErrorHeaders.with(headers, failure, ErrorHeaders.FILTER_STAGE, null) : headers;
    }
}

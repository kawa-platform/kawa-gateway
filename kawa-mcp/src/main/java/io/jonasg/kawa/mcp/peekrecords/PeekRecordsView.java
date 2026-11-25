package io.jonasg.kawa.mcp.peekrecords;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.jspecify.annotations.Nullable;

import java.util.List;

/// The `peek_records` result: what was inspected, how far the inspection got, and the records it returns.
///
/// @param topic              the physical topic inspected
/// @param partitions         the partitions selected
/// @param limit              the most records the call allowed to be inspected, over all partitions
/// @param format             the value format asked for
/// @param position           where inspection started: `latest` (the latest records, read back from the end),
///                           `offset` or `time`
/// @param startTime          `start_time` as given, normalised to UTC
/// @param until              `until` as given, normalised to UTC
/// @param inspectedAt        when the end offsets were read; records written later are not part of the inspection
/// @param recordsInspected   the records read, all partitions together; at most `limit`
/// @param recordsReturned    the records in `records`
/// @param outsideTimeRange   inspected records whose timestamp lies outside `[startTime, until)`, which are not
///                           returned
/// @param matches            inspected records that match the search; absent without a search
/// @param unsearchable       inspected records the search could not decide (binary key, undecodable value); absent
///                           without a search
/// @param exhaustive         whether every offset of the requested range was inspected in every selected partition
/// @param limitReached       whether inspection stopped because `limit` records were read
/// @param timedOut           whether inspection stopped at its time limit
/// @param truncated          whether any returned key or value was shortened to keep the result small
/// @param summary            the outcome in one sentence, to relay to the user
/// @param ranges             per partition: the offsets it holds, the range asked for and the part inspected
/// @param records            the records returned: every inspected record in the time range, or with a search
///                           the matching and unsearchable ones; ordered by timestamp, then partition and offset
/// @param limitations        what the result does not tell
/// @param formatUndetermined set when values had to be decoded and the format of some could not be detected
@JsonInclude(JsonInclude.Include.NON_NULL)
record PeekRecordsView(
        String topic,
        List<Integer> partitions,
        int limit,
        String format,
        String position,
        @Nullable String startTime,
        @Nullable String until,
        String inspectedAt,
        int recordsInspected,
        int recordsReturned,
        int outsideTimeRange,
        @Nullable Integer matches,
        @Nullable Integer unsearchable,
        boolean exhaustive,
        boolean limitReached,
        boolean timedOut,
        boolean truncated,
        String summary,
        List<RangeView> ranges,
        List<RecordView> records,
        List<String> limitations,
        @Nullable FormatQuestion formatUndetermined
) {

    /// One partition's offsets. Kafka offsets are per partition and may have gaps, so an offset count is an
    /// upper bound on the records it spans.
    ///
    /// @param partition            the partition
    /// @param logStartOffset       the earliest offset still in the log; absent when the partition could not be read
    /// @param logEndOffset         the offset the next record will get
    /// @param requestedStartOffset the start offset asked for
    /// @param requestedEndOffset   the exclusive end offset asked for
    /// @param rangeStartOffset     the first offset of the range inspected, after resolving times and clamping to the log
    /// @param rangeEndOffset       the exclusive end of that range
    /// @param offsetsInRange       `rangeEndOffset - rangeStartOffset`, counted without reading records: an upper bound
    ///                             on the records in the range
    /// @param inspectedFromOffset  the first offset covered by inspection
    /// @param inspectedToOffset    the exclusive end of the offsets covered
    /// @param recordsInspected     the records read from the partition
    /// @param complete             whether every offset of the range was covered
    /// @param error                why the partition could not be (fully) read
    /// @param notes                adjustments made, such as a start offset below the earliest one; absent when none
    @JsonInclude(JsonInclude.Include.NON_NULL)
    record RangeView(
            int partition,
            @Nullable Long logStartOffset,
            @Nullable Long logEndOffset,
            @Nullable Long requestedStartOffset,
            @Nullable Long requestedEndOffset,
            @Nullable Long rangeStartOffset,
            @Nullable Long rangeEndOffset,
            @Nullable Long offsetsInRange,
            @Nullable Long inspectedFromOffset,
            @Nullable Long inspectedToOffset,
            int recordsInspected,
            boolean complete,
            @Nullable String error,
            @Nullable List<String> notes
    ) {
    }

    /// One record.
    ///
    /// @param timestamp      the Kafka record timestamp (ISO-8601, UTC): when the producer created it
    ///                       (`CreateTime`) or the broker appended it (`LogAppendTime`), not when a consumer read it
    /// @param key            the key: UTF-8 text, or Base64 when it is not text; absent for a null key
    /// @param keyEncoding    `utf8` or `base64`
    /// @param valueSize      the value's size in bytes; absent for a null value
    /// @param valueStatus    `null` (a null value, e.g. a tombstone), `not_decoded` (not asked for), `decoded`,
    ///                       `failed` or `unidentified`
    /// @param format         the format the value was decoded in or detected as
    /// @param valueError     why the value did not decode
    /// @param value          the decoded value; a string of its first characters when `valueTruncated`
    /// @param raw            the value bytes as Base64, when asked for
    /// @param matched        with a search: whether the record matches; absent when it could not be searched
    /// @param searchError    why the record could not be searched
    @JsonInclude(JsonInclude.Include.NON_NULL)
    record RecordView(
            String topic,
            int partition,
            long offset,
            @Nullable String timestamp,
            String timestampType,
            @Nullable String key,
            @Nullable String keyEncoding,
            @Nullable Boolean keyTruncated,
            @Nullable Integer valueSize,
            String valueStatus,
            @Nullable String format,
            @Nullable String valueError,
            @Nullable Object value,
            @Nullable Boolean valueTruncated,
            @Nullable String raw,
            @Nullable Boolean rawTruncated,
            @Nullable Boolean matched,
            @Nullable String searchError
    ) {
    }

    /// Values whose format could not be detected. This server cannot ask the user itself (its stateless MCP
    /// transport offers no elicitation), so the agent asks and calls again with `format`.
    ///
    /// @param records the records whose format was not detected
    /// @param message what to do about it
    /// @param options the formats `format` accepts
    record FormatQuestion(int records, String message, List<String> options) {
    }

    /// The JSON Schema of a [PeekRecordsView], published as the tool's `outputSchema`.
    static final String SCHEMA = """
            {
              "type": "object",
              "properties": {
                "topic": { "type": "string" },
                "partitions": { "type": "array", "items": { "type": "integer" } },
                "limit": { "type": "integer" },
                "format": { "type": "string" },
                "position": { "type": "string", "enum": ["latest", "offset", "time"] },
                "startTime": { "type": "string" },
                "until": { "type": "string" },
                "inspectedAt": { "type": "string" },
                "recordsInspected": { "type": "integer" },
                "recordsReturned": { "type": "integer" },
                "outsideTimeRange": { "type": "integer" },
                "matches": { "type": "integer" },
                "unsearchable": { "type": "integer" },
                "exhaustive": { "type": "boolean" },
                "limitReached": { "type": "boolean" },
                "timedOut": { "type": "boolean" },
                "truncated": { "type": "boolean" },
                "summary": { "type": "string" },
                "ranges": {
                  "type": "array",
                  "items": {
                    "type": "object",
                    "properties": {
                      "partition": { "type": "integer" },
                      "logStartOffset": { "type": "integer" },
                      "logEndOffset": { "type": "integer" },
                      "requestedStartOffset": { "type": "integer" },
                      "requestedEndOffset": { "type": "integer" },
                      "rangeStartOffset": { "type": "integer" },
                      "rangeEndOffset": { "type": "integer" },
                      "offsetsInRange": { "type": "integer" },
                      "inspectedFromOffset": { "type": "integer" },
                      "inspectedToOffset": { "type": "integer" },
                      "recordsInspected": { "type": "integer" },
                      "complete": { "type": "boolean" },
                      "error": { "type": "string" },
                      "notes": { "type": "array", "items": { "type": "string" } }
                    },
                    "required": ["partition"],
                    "additionalProperties": false
                  }
                },
                "records": {
                  "type": "array",
                  "items": {
                    "type": "object",
                    "properties": {
                      "topic": { "type": "string" },
                      "partition": { "type": "integer" },
                      "offset": { "type": "integer" },
                      "timestamp": { "type": "string" },
                      "timestampType": { "type": "string" },
                      "key": { "type": "string" },
                      "keyEncoding": { "type": "string", "enum": ["utf8", "base64"] },
                      "keyTruncated": { "type": "boolean" },
                      "valueSize": { "type": "integer" },
                      "valueStatus": { "type": "string", "enum": ["null", "not_decoded", "decoded", "failed", "unidentified"] },
                      "format": { "type": "string" },
                      "valueError": { "type": "string" },
                      "value": {},
                      "valueTruncated": { "type": "boolean" },
                      "raw": { "type": "string" },
                      "rawTruncated": { "type": "boolean" },
                      "matched": { "type": "boolean" },
                      "searchError": { "type": "string" }
                    },
                    "required": ["topic", "partition", "offset", "timestampType", "valueStatus"],
                    "additionalProperties": false
                  }
                },
                "limitations": { "type": "array", "items": { "type": "string" } },
                "formatUndetermined": {
                  "type": "object",
                  "properties": {
                    "records": { "type": "integer" },
                    "message": { "type": "string" },
                    "options": { "type": "array", "items": { "type": "string" } }
                  },
                  "required": ["records", "message", "options"],
                  "additionalProperties": false
                }
              },
              "required": [
                "topic", "partitions", "limit", "format", "position", "inspectedAt", "recordsInspected",
                "recordsReturned", "outsideTimeRange", "exhaustive", "limitReached", "timedOut", "truncated", "summary",
                "ranges", "records", "limitations"
              ],
              "additionalProperties": false
            }
            """;
}

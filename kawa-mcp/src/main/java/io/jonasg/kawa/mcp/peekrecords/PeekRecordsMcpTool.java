package io.jonasg.kawa.mcp.peekrecords;

import io.jonasg.kawa.broker.RecordReader;
import io.jonasg.kawa.broker.RecordReader.FetchedRecord;
import io.jonasg.kawa.broker.RecordReader.LogRange;
import io.jonasg.kawa.http.topics.TopicService;
import io.jonasg.kawa.mcp.McpTool;
import io.jonasg.kawa.mcp.peekrecords.PeekRecordsView.FormatQuestion;
import io.jonasg.kawa.mcp.peekrecords.PeekRecordsView.RangeView;
import io.jonasg.kawa.mcp.peekrecords.PeekRecordsView.RecordView;
import io.jonasg.kawa.virtualtopic.filter.AvroSchemaResolver;
import io.jonasg.kawa.virtualtopic.filter.SchemaRegistries;
import io.modelcontextprotocol.json.McpJsonMapper;
import io.modelcontextprotocol.spec.McpSchema.CallToolRequest;
import io.modelcontextprotocol.spec.McpSchema.ToolAnnotations;
import org.jspecify.annotations.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/// The `peek_records` tool: reads a bounded number of records of a physical topic for an agent to inspect,
/// without joining a consumer group or committing offsets (see [RecordReader]).
///
/// Inspection reads at most `limit` records over all selected partitions, shared fairly between them (see
/// [PartitionScan#scanAll]). Without a start it reads the latest records; with `start_offset(s)` or
/// `start_time` it reads forward from there, up to `end_offset(s)`, `until` or the end offset read when the
/// call started. Times resolve to offsets per partition with the broker's timestamp index.
///
/// Values are decoded only when asked to show them or search them (see [Payloads]); keys, offsets and timestamps
/// never need decoding. The view reports how far inspection got, so a search over part of a range is never
/// presented as covering all of it.
public final class PeekRecordsMcpTool extends McpTool {

    private static final String NAME = "peek_records";
    /// The time each offset lookup, and all reads of a call together, may take.
    static final Duration TIMEOUT = Duration.ofSeconds(10);
    static final int MAX_VALUE_CHARS = 4_000;
    static final int MAX_KEY_CHARS = 512;
    /// The characters all returned values and raw payloads may take together, so a result stays small.
    static final int MAX_TOTAL_CHARS = 200_000;

    private static final String INPUT_SCHEMA = """
            {
              "type": "object",
              "properties": {
                "topic": {
                  "type": "string",
                  "description": "Name of the physical Kafka topic to inspect"
                },
                "partitions": {
                  "type": "array",
                  "items": { "type": "integer", "minimum": 0 },
                  "description": "Partitions to inspect; all partitions when omitted"
                },
                "limit": {
                  "type": "integer",
                  "minimum": 1,
                  "maximum": 500,
                  "default": 50,
                  "description": "Most records to inspect, over all selected partitions together (not per partition)"
                },
                "format": {
                  "type": "string",
                  "enum": ["auto", "json", "xml", "avro", "string", "raw"],
                  "default": "auto",
                  "description": "How to decode values: auto detects per record; raw returns the bytes as Base64"
                },
                "include_values": {
                  "type": "boolean",
                  "default": false,
                  "description": "Decode and return record values. Leave off for questions about record existence, counts, timestamps, offsets or keys"
                },
                "include_raw": {
                  "type": "boolean",
                  "default": false,
                  "description": "Also return each value's bytes as Base64"
                },
                "start_offset": {
                  "type": "integer",
                  "minimum": 0,
                  "description": "Inclusive start offset; only with a single selected partition"
                },
                "start_offsets": {
                  "type": "object",
                  "additionalProperties": { "type": "integer", "minimum": 0 },
                  "description": "Inclusive start offset per partition, e.g. {\\"0\\": 1200, \\"1\\": 850}; selects those partitions when partitions is omitted"
                },
                "start_time": {
                  "type": "string",
                  "format": "date-time",
                  "description": "Start at the first record whose Kafka timestamp is at or after this ISO-8601 time"
                },
                "end_offset": {
                  "type": "integer",
                  "minimum": 0,
                  "description": "Exclusive end offset; only with a single selected partition"
                },
                "end_offsets": {
                  "type": "object",
                  "additionalProperties": { "type": "integer", "minimum": 0 },
                  "description": "Exclusive end offset per partition"
                },
                "until": {
                  "type": "string",
                  "format": "date-time",
                  "description": "Exclusive upper bound on the Kafka record timestamp, ISO-8601"
                },
                "key_contains": {
                  "type": "string",
                  "description": "Return only records whose UTF-8 key contains this text, ignoring case. Values are not decoded for it"
                },
                "value_contains": {
                  "type": "string",
                  "description": "Return only records whose decoded value contains this text in any nested field value, ignoring case"
                }
              },
              "required": ["topic"],
              "additionalProperties": false
            }""";

    private final McpJsonMapper mapper;
    private final TopicService topicService;
    private final RecordReader recordReader;
    private final SchemaRegistries schemaRegistries;

    public PeekRecordsMcpTool(McpJsonMapper mapper, TopicService topicService, RecordReader recordReader,
                              SchemaRegistries schemaRegistries) {
        super(mapper);
        this.mapper = mapper;
        this.topicService = topicService;
        this.recordReader = recordReader;
        this.schemaRegistries = schemaRegistries;
    }

    @Override
    protected String name() {
        return NAME;
    }

    @Override
    protected String description() {
        return """
                Reads a bounded number of records of a physical Kafka topic, read-only: it joins no consumer group, \
                commits no offsets and does not affect any application's consumers.

                Metadata and keys need no value decoding; leave include_values off for: whether records arrived \
                (in a time range: start_time/until), how many (ranges[].offsetsInRange counts offsets without \
                reading records, an upper bound), which partitions are active, timestamps, offsets, and key \
                searches (key_contains).
                Payload questions decode values: set include_values to see them, or value_contains to search \
                them, including nested JSON, XML and Avro fields. format auto detects JSON, XML, schema registry \
                Avro and text per record; when it cannot, the result's formatUndetermined says so: ask the user \
                for the format and call again with format set.

                limit (default 50, max 500) caps the records inspected over all selected partitions together, \
                shared fairly between them. Without a start position it reads the latest records; \
                start_offset/start_offsets or start_time read forward from there, up to end_offset/end_offsets, \
                until, or the end of the log. Offsets are per partition and records of different partitions \
                have no global order.

                Results are bounded: check exhaustive and limitReached before concluding. A search that is not \
                exhaustive found no match only among the records inspected, never in the whole topic or time \
                range. Relay the summary field. Virtual topics are not accepted: peek their physical topic.""";
    }

    @Override
    protected String inputSchema() {
        return INPUT_SCHEMA;
    }

    @Override
    protected String outputSchema() {
        return PeekRecordsView.SCHEMA;
    }

    @Override
    protected ToolAnnotations annotations() {
        return ToolAnnotations.builder()
                .title("Peek records")
                .readOnlyHint(true)
                .destructiveHint(false)
                .idempotentHint(true)
                .openWorldHint(false)
                .build();
    }

    @Override
    protected PeekRecordsView call(CallToolRequest request) {
        var peek = PeekRequest.parse(request.arguments());
        var topic = topicService.describeTopic(peek.topic());
        if (topic.virtualTopic() != null) {
            throw new IllegalArgumentException("'" + peek.topic() + "' is a virtual topic over the physical topic '"
                    + topic.physicalTopic() + "'; peek_records reads physical topics, so call it with topic '"
                    + topic.physicalTopic() + "' (the virtual topic's filter and transformations are not applied)");
        }
        if (topic.partitions() == 0) {
            throw new IllegalArgumentException("the broker reports no partitions for topic '" + peek.topic() + "'");
        }
        var partitions = peek.selectPartitions(topic.partitions());
        var inspectedAt = Instant.now();
        boolean timedOut;
        List<PartitionScan> scans;
        try (var session = recordReader.open()) {
            scans = plan(session, peek, partitions);
            timedOut = PartitionScan.scanAll(session, peek.topic(), scans, peek.limit(), TIMEOUT);
        }
        return view(peek, partitions, inspectedAt, scans, timedOut);
    }

    /// The range each partition is inspected over, resolved from the offsets and times asked for and clamped to
    /// the offsets the partition holds.
    private static List<PartitionScan> plan(RecordReader.Session session, PeekRequest peek, List<Integer> partitions) {
        Map<Integer, LogRange> logs;
        Map<Integer, Long> startOffsets = Map.of();
        Map<Integer, Long> untilOffsets = Map.of();
        try {
            logs = session.logRanges(peek.topic(), partitions, TIMEOUT);
            if (peek.startTime() != null) {
                startOffsets = session.offsetsForTime(peek.topic(), partitions, peek.startTime(), TIMEOUT);
            }
            if (peek.until() != null) {
                untilOffsets = session.offsetsForTime(peek.topic(), partitions, peek.until(), TIMEOUT);
            }
        } catch (RuntimeException e) {
            String error = "could not read the partition's offsets from the broker: " + PartitionScan.message(e);
            return partitions.stream().map(partition -> PartitionScan.failed(partition, error)).toList();
        }
        List<PartitionScan> scans = new ArrayList<>();
        for (int partition : partitions) {
            var log = logs.get(partition);
            if (log == null) {
                scans.add(PartitionScan.failed(partition, "the broker reported no offsets for partition " + partition));
                continue;
            }
            List<String> notes = new ArrayList<>();
            Long requestedStart = peek.requestedStart(partition);
            Long requestedEnd = peek.requestedEnd(partition);
            long end;
            if (requestedEnd != null) {
                end = clampEnd(requestedEnd, log, notes);
            } else if (peek.until() != null) {
                Long found = untilOffsets.get(partition);
                end = found == null ? log.end() : found;
                notes.add(found == null
                        ? "no record has a timestamp at or after until, so the range runs to the end offset " + log.end()
                        : "until resolved to offset " + found + ", the first record with a timestamp at or after it");
            } else {
                end = log.end();
            }
            long start;
            if (requestedStart != null) {
                start = clampStart(requestedStart, log, notes);
            } else if (peek.startTime() != null) {
                Long found = startOffsets.get(partition);
                start = found == null ? log.end() : found;
                notes.add(found == null
                        ? "no record has a timestamp at or after start_time, so there is nothing to inspect"
                        : "start_time resolved to offset " + found + ", the first record with a timestamp at or after it");
            } else {
                start = log.start();
            }
            boolean forward = peek.position() != PeekRequest.Position.LATEST;
            scans.add(new PartitionScan(partition, log, requestedStart, requestedEnd, start, end, forward, notes));
        }
        return scans;
    }

    private static long clampStart(long requested, LogRange log, List<String> notes) {
        if (requested < log.start()) {
            notes.add("start offset " + requested + " is below the earliest available offset " + log.start()
                    + " (removed by retention or compaction, or never written); inspection starts at " + log.start());
            return log.start();
        }
        if (requested > log.end()) {
            notes.add("start offset " + requested + " is beyond the end offset " + log.end()
                    + "; no record has been written there yet");
            return log.end();
        }
        return requested;
    }

    private static long clampEnd(long requested, LogRange log, List<String> notes) {
        if (requested > log.end()) {
            notes.add("end offset " + requested + " is beyond the end offset " + log.end() + "; inspection stops at "
                    + log.end());
            return log.end();
        }
        if (requested < log.start()) {
            notes.add("end offset " + requested + " is below the earliest available offset " + log.start()
                    + "; the requested records were removed by retention or compaction");
            return log.start();
        }
        return requested;
    }

    private PeekRecordsView view(PeekRequest peek, List<Integer> partitions, Instant inspectedAt,
                                 List<PartitionScan> scans, boolean timedOut) {
        AvroSchemaResolver avroSchemas = schemaRegistries.avroSchemasFor(peek.topic());
        List<FetchedRecord> inspected = scans.stream()
                .flatMap(scan -> scan.records.stream())
                .sorted(Comparator.comparingLong(FetchedRecord::timestamp)
                        .thenComparingInt(FetchedRecord::partition)
                        .thenComparingLong(FetchedRecord::offset))
                .toList();
        var output = new Output();
        int outsideTimeRange = 0;
        int matches = 0;
        int unsearchable = 0;
        int undetermined = 0;
        List<RecordView> records = new ArrayList<>();
        for (FetchedRecord record : inspected) {
            if (!withinTime(record, peek)) {
                outsideTimeRange++;
                continue;
            }
            var key = output.key(record.key());
            Boolean matched = null;
            String searchError = null;
            boolean keyMismatch = false;
            if (peek.keyContains() != null) {
                if (record.key() == null) {
                    keyMismatch = true;
                } else if (!"utf8".equals(key.encoding)) {
                    searchError = "the key is binary, not UTF-8 text, so key_contains cannot search it";
                } else {
                    keyMismatch = !Payloads.contains(Payloads.utf8(record.key()), peek.keyContains());
                }
            }
            Payloads.Decoded decoded = null;
            if (record.value() != null && (peek.includeValues() || (peek.valueContains() != null && !keyMismatch))) {
                decoded = Payloads.decode(record.value(), peek.format(), avroSchemas);
                if ("unidentified".equals(decoded.status())) {
                    undetermined++;
                }
            }
            boolean valueMismatch = false;
            if (peek.valueContains() != null && !keyMismatch) {
                if (record.value() == null) {
                    valueMismatch = true;
                } else if (decoded != null && decoded.decoded()) {
                    valueMismatch = !Payloads.contains(decoded.value(), peek.valueContains());
                } else if (searchError == null && decoded != null) {
                    searchError = "the value could not be decoded, so value_contains cannot search it: " + decoded.error();
                }
            }
            if (peek.searches()) {
                matched = keyMismatch || valueMismatch ? Boolean.FALSE : searchError == null ? Boolean.TRUE : null;
                if (matched == null) {
                    unsearchable++;
                } else if (matched) {
                    matches++;
                } else {
                    continue;
                }
            }
            records.add(output.record(peek, record, key, decoded, matched, searchError));
        }
        boolean exhaustive = !timedOut && scans.stream().allMatch(PartitionScan::complete);
        boolean limitReached = !exhaustive && inspected.size() >= peek.limit();
        var summary = new Summary(peek, partitions, inspected.size(), exhaustive, limitReached, timedOut,
                scans.stream().filter(scan -> scan.error != null).map(scan -> scan.partition).toList(),
                matches);
        return new PeekRecordsView(
                peek.topic(),
                partitions,
                peek.limit(),
                peek.format().label(),
                peek.position().name().toLowerCase(Locale.ROOT),
                peek.startTime() == null ? null : peek.startTime().toString(),
                peek.until() == null ? null : peek.until().toString(),
                inspectedAt.toString(),
                inspected.size(),
                records.size(),
                outsideTimeRange,
                peek.searches() ? matches : null,
                peek.searches() ? unsearchable : null,
                exhaustive,
                limitReached,
                timedOut,
                output.truncated,
                summary.text(unsearchable),
                scans.stream().map(PeekRecordsMcpTool::range).toList(),
                records,
                limitations(peek, output.truncated),
                peek.format() == PayloadFormat.AUTO && undetermined > 0 ? formatQuestion(undetermined) : null
        );
    }

    private static boolean withinTime(FetchedRecord record, PeekRequest peek) {
        return (peek.startTime() == null || record.timestamp() >= peek.startTime().toEpochMilli())
                && (peek.until() == null || record.timestamp() < peek.until().toEpochMilli());
    }

    private static RangeView range(PartitionScan scan) {
        var log = scan.log;
        return new RangeView(
                scan.partition,
                log == null ? null : log.start(),
                log == null ? null : log.end(),
                scan.requestedStart,
                scan.requestedEnd,
                log == null ? null : scan.rangeStart,
                log == null ? null : scan.rangeEnd,
                log == null ? null : scan.rangeEnd - scan.rangeStart,
                log == null ? null : scan.coveredFrom,
                log == null ? null : scan.coveredTo,
                scan.records.size(),
                scan.complete(),
                scan.error,
                scan.notes.isEmpty() ? null : List.copyOf(scan.notes));
    }

    private static FormatQuestion formatQuestion(int records) {
        return new FormatQuestion(records,
                "The value format of " + records + " record(s) could not be detected. Ask the user how the values "
                        + "are encoded and call peek_records again with format set to one of json, xml, avro, string "
                        + "or raw. This server cannot ask the user itself: MCP elicitation needs a session, and Kawa "
                        + "serves MCP statelessly.",
                List.of("json", "xml", "avro", "string", "raw"));
    }

    private static List<String> limitations(PeekRequest peek, boolean truncated) {
        List<String> limitations = new ArrayList<>();
        limitations.add("Offsets are per partition; records of different partitions have no global order, so records "
                + "are ordered by timestamp on a best-effort basis.");
        limitations.add("offsetsInRange counts offset positions, not records: compaction, transaction markers and "
                + "retention leave gaps, so it is an upper bound on the records in a range.");
        limitations.add("Records written after inspectedAt are not part of this inspection.");
        if (peek.startTime() != null || peek.until() != null) {
            limitations.add("Time ranges use Kafka record timestamps, not the time an application received or processed "
                    + "a record. With CreateTime the producer sets them and they can be out of order, so a time range "
                    + "resolves to offsets on a best-effort basis; records outside it are counted in outsideTimeRange.");
        }
        if (peek.searches()) {
            limitations.add("Searches are case-insensitive substring matches over UTF-8 keys and over decoded value "
                    + "fields (values, not field names), and cover only the records inspected.");
        }
        if (truncated) {
            limitations.add("Keys over " + MAX_KEY_CHARS + " and values over " + MAX_VALUE_CHARS + " characters are "
                    + "shortened, and values stop being returned after " + MAX_TOTAL_CHARS + " characters in all.");
        }
        return limitations;
    }

    /// The one-sentence outcome, worded so that an inspection of part of a range is not taken for all of it.
    private record Summary(PeekRequest peek, List<Integer> partitions, int inspected, boolean exhaustive,
                           boolean limitReached, boolean timedOut, List<Integer> failed, int matches) {

        String text(int unsearchable) {
            var text = new StringBuilder("Inspected ").append(inspected).append(" record(s) from ")
                    .append(partitions.size()).append(" partition(s) of '").append(peek.topic()).append("'");
            if (peek.position() == PeekRequest.Position.LATEST && !exhaustive) {
                text.append(", the latest ones");
            }
            if (peek.searches()) {
                text.append("; ").append(matches).append(" matched");
                if (unsearchable > 0) {
                    text.append(" and ").append(unsearchable).append(" could not be searched");
                }
            }
            if (exhaustive) {
                return text.append(". Every record in the requested range was inspected.").toString();
            }
            List<String> reasons = new ArrayList<>();
            if (limitReached) {
                reasons.add("stopped at the limit of " + peek.limit() + " records");
            }
            if (timedOut) {
                reasons.add("stopped at the time limit");
            }
            if (!failed.isEmpty()) {
                reasons.add("partition(s) " + failed + " could not be read");
            }
            if (reasons.isEmpty()) {
                reasons.add("part of the requested range was not covered");
            }
            text.append(". Not exhaustive: ").append(String.join(", ", reasons)).append(".");
            if (peek.searches() && matches == 0) {
                text.append(" No match among the records inspected; this does not show that no matching record exists.");
            }
            return text.toString();
        }
    }

    /// A key as returned: text when it is printable UTF-8, else Base64.
    private record Key(@Nullable String text, @Nullable String encoding, boolean truncated) {
    }

    /// Builds the returned keys and values within the size limits, remembering whether anything was shortened.
    private final class Output {

        private int charsLeft = MAX_TOTAL_CHARS;
        private boolean truncated;

        Key key(byte @Nullable [] bytes) {
            if (bytes == null) {
                return new Key(null, null, false);
            }
            String text = Payloads.utf8(bytes);
            String encoding = "utf8";
            if (text == null || !Payloads.printable(text)) {
                text = Base64.getEncoder().encodeToString(bytes);
                encoding = "base64";
            }
            if (text.length() > MAX_KEY_CHARS) {
                truncated = true;
                return new Key(text.substring(0, MAX_KEY_CHARS), encoding, true);
            }
            return new Key(text, encoding, false);
        }

        RecordView record(PeekRequest peek, FetchedRecord record, Key key, Payloads.@Nullable Decoded decoded,
                          @Nullable Boolean matched, @Nullable String searchError) {
            String status = record.value() == null ? "null" : decoded == null ? "not_decoded" : decoded.status();
            Object value = null;
            Boolean valueTruncated = null;
            if (decoded != null && decoded.decoded()) {
                String json = decoded.value() instanceof String text ? text : json(decoded.value());
                var shown = shorten(json, decoded.value());
                value = shown.value;
                valueTruncated = shown.truncated ? Boolean.TRUE : null;
            }
            String raw = null;
            Boolean rawTruncated = null;
            if (peek.includeRaw() && record.value() != null) {
                String base64 = Base64.getEncoder().encodeToString(record.value());
                var shown = shorten(base64, base64);
                raw = (String) shown.value;
                rawTruncated = shown.truncated ? Boolean.TRUE : null;
            }
            return new RecordView(
                    peek.topic(),
                    record.partition(),
                    record.offset(),
                    record.timestamp() < 0 ? null : Instant.ofEpochMilli(record.timestamp()).toString(),
                    record.timestampType(),
                    key.text,
                    key.encoding,
                    key.truncated ? Boolean.TRUE : null,
                    record.value() == null ? null : record.value().length,
                    status,
                    decoded == null ? null : decoded.format(),
                    decoded == null ? null : decoded.error(),
                    value,
                    valueTruncated,
                    raw,
                    rawTruncated,
                    matched,
                    searchError);
        }

        /// `value` when its text form `text` fits the limits, else the start of `text`.
        private Shown shorten(String text, @Nullable Object value) {
            int room = Math.min(MAX_VALUE_CHARS, charsLeft);
            if (text.length() <= room) {
                charsLeft -= text.length();
                return new Shown(value, false);
            }
            truncated = true;
            charsLeft -= room;
            return new Shown(room == 0 ? null : text.substring(0, room), true);
        }

        private String json(@Nullable Object value) {
            try {
                return mapper.writeValueAsString(value);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private record Shown(@Nullable Object value, boolean truncated) {
    }
}

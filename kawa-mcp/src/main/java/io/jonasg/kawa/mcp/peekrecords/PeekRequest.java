package io.jonasg.kawa.mcp.peekrecords;

import org.jspecify.annotations.Nullable;

import java.math.BigInteger;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/// The validated arguments of one `peek_records` call. [#parse] rejects anything malformed, out of range
/// or conflicting with an [IllegalArgumentException] naming the argument, so the agent can correct its call.
///
/// @param partitions the partitions asked for, sorted and without duplicates; `null` when the call named none
/// @param endOffset  the exclusive end offset of the single selected partition; `endOffsets` per partition
record PeekRequest(
        String topic,
        @Nullable List<Integer> partitions,
        int limit,
        PayloadFormat format,
        @Nullable Long startOffset,
        @Nullable Map<Integer, Long> startOffsets,
        @Nullable Instant startTime,
        @Nullable Long endOffset,
        @Nullable Map<Integer, Long> endOffsets,
        @Nullable Instant until,
        @Nullable String keyContains,
        @Nullable String valueContains,
        boolean includeValues,
        boolean includeRaw
) {

    static final int DEFAULT_LIMIT = 50;
    static final int MAX_LIMIT = 500;

    private static final Set<String> ARGUMENTS = Set.of("topic", "partitions", "limit", "format", "start_offset",
            "start_offsets", "start_time", "end_offset", "end_offsets", "until", "key_contains", "value_contains",
            "include_values", "include_raw");

    /// How a starting point was asked for.
    enum Position {
        /// No start: the latest records, read back from the end of the range.
        LATEST,
        OFFSET,
        TIME
    }

    static PeekRequest parse(@Nullable Map<String, Object> arguments) {
        Map<String, Object> args = arguments == null ? Map.of() : arguments;
        var unknown = new TreeSet<>(args.keySet());
        unknown.removeAll(ARGUMENTS);
        if (!unknown.isEmpty()) {
            throw new IllegalArgumentException("unknown argument(s) " + unknown + "; accepted are " + new TreeSet<>(ARGUMENTS));
        }
        var request = new PeekRequest(
                requiredString(args, "topic"),
                partitions(args.get("partitions")),
                limit(args.get("limit")),
                format(args.get("format")),
                offset(args.get("start_offset"), "start_offset"),
                offsets(args.get("start_offsets"), "start_offsets"),
                time(args.get("start_time"), "start_time"),
                offset(args.get("end_offset"), "end_offset"),
                offsets(args.get("end_offsets"), "end_offsets"),
                time(args.get("until"), "until"),
                optionalString(args, "key_contains"),
                optionalString(args, "value_contains"),
                bool(args.get("include_values"), "include_values"),
                bool(args.get("include_raw"), "include_raw"));
        request.validate();
        return request;
    }

    private void validate() {
        exclusive(startOffset != null, "start_offset", startOffsets != null, "start_offsets");
        exclusive(endOffset != null, "end_offset", endOffsets != null, "end_offsets");
        exclusive(startOffset != null, "start_offset", startTime != null, "start_time");
        exclusive(startOffsets != null, "start_offsets", startTime != null, "start_time");
        exclusive(endOffset != null, "end_offset", until != null, "until");
        exclusive(endOffsets != null, "end_offsets", until != null, "until");
        if (startTime != null && until != null && !until.isAfter(startTime)) {
            throw new IllegalArgumentException("until must be after start_time");
        }
        if (startOffset != null && endOffset != null && endOffset <= startOffset) {
            throw new IllegalArgumentException("end_offset is exclusive and must be greater than start_offset");
        }
        if (startOffsets != null && endOffsets != null) {
            endOffsets.forEach((partition, end) -> {
                Long start = startOffsets.get(partition);
                if (start != null && end <= start) {
                    throw new IllegalArgumentException("end_offsets." + partition
                            + " is exclusive and must be greater than start_offsets." + partition);
                }
            });
        }
        if (partitions != null) {
            mapWithin(startOffsets, "start_offsets");
            mapWithin(endOffsets, "end_offsets");
            if (startOffsets != null && !startOffsets.keySet().containsAll(partitions)) {
                var missing = new TreeSet<>(partitions);
                missing.removeAll(startOffsets.keySet());
                throw new IllegalArgumentException("start_offsets gives no start offset for selected partition(s) " + missing);
            }
        }
        if (valueContains != null && format == PayloadFormat.RAW) {
            throw new IllegalArgumentException("value_contains searches decoded values; it cannot be used with format raw");
        }
    }

    /// The partitions to inspect, of a topic with `partitionCount` partitions: those named, else those an
    /// offset map names, else all of them.
    ///
    /// @throws IllegalArgumentException when a partition does not exist, or a single-partition argument is used
    ///                                  with several partitions
    List<Integer> selectPartitions(int partitionCount) {
        List<Integer> selected;
        if (partitions != null) {
            selected = partitions;
        } else if (startOffsets != null) {
            selected = List.copyOf(startOffsets.keySet());
        } else if (endOffsets != null) {
            selected = List.copyOf(endOffsets.keySet());
        } else {
            selected = new ArrayList<>();
            for (int partition = 0; partition < partitionCount; partition++) {
                selected.add(partition);
            }
        }
        var invalid = selected.stream().filter(partition -> partition >= partitionCount).toList();
        if (!invalid.isEmpty()) {
            throw new IllegalArgumentException("topic '" + topic + "' has no partition(s) " + invalid
                    + "; its partitions are 0 to " + (partitionCount - 1));
        }
        if (selected.size() != 1) {
            if (startOffset != null) {
                throw new IllegalArgumentException("start_offset applies to a single partition, but " + selected.size()
                        + " are selected; name one in partitions, or use start_offsets");
            }
            if (endOffset != null) {
                throw new IllegalArgumentException("end_offset applies to a single partition, but " + selected.size()
                        + " are selected; name one in partitions, or use end_offsets");
            }
        }
        return selected;
    }

    /// The start offset asked for `partition`, or `null` when none was.
    @Nullable Long requestedStart(int partition) {
        return startOffset != null ? startOffset : startOffsets == null ? null : startOffsets.get(partition);
    }

    /// The exclusive end offset asked for `partition`, or `null` when none was.
    @Nullable Long requestedEnd(int partition) {
        return endOffset != null ? endOffset : endOffsets == null ? null : endOffsets.get(partition);
    }

    Position position() {
        if (startTime != null) {
            return Position.TIME;
        }
        return startOffset != null || startOffsets != null ? Position.OFFSET : Position.LATEST;
    }

    /// Whether values must be decoded: to search them, or because the call asked to see them.
    boolean decodesValues() {
        return valueContains != null || includeValues;
    }

    boolean searches() {
        return keyContains != null || valueContains != null;
    }

    private void mapWithin(@Nullable Map<Integer, Long> offsets, String name) {
        if (offsets != null && partitions != null && !partitions.containsAll(offsets.keySet())) {
            var outside = new TreeSet<>(offsets.keySet());
            outside.removeAll(partitions);
            throw new IllegalArgumentException(name + " names partition(s) " + outside + " that are not in partitions " + partitions);
        }
    }

    private static void exclusive(boolean first, String firstName, boolean second, String secondName) {
        if (first && second) {
            throw new IllegalArgumentException(firstName + " and " + secondName + " cannot be combined");
        }
    }

    private static String requiredString(Map<String, Object> args, String name) {
        var value = optionalString(args, name);
        if (value == null) {
            throw new IllegalArgumentException(name + " is required");
        }
        return value;
    }

    private static @Nullable String optionalString(Map<String, Object> args, String name) {
        return switch (args.get(name)) {
            case null -> null;
            case String value when value.isEmpty() -> null;
            case String value -> value;
            default -> throw new IllegalArgumentException(name + " must be a string");
        };
    }

    private static @Nullable List<Integer> partitions(@Nullable Object value) {
        if (value == null) {
            return null;
        }
        if (!(value instanceof List<?> list) || list.isEmpty()) {
            throw new IllegalArgumentException("partitions must be a non-empty array of partition numbers");
        }
        var partitions = new TreeSet<Integer>();
        for (Object element : list) {
            long partition = integer(element, "partitions");
            if (partition < 0 || partition > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("partitions holds the invalid partition " + partition);
            }
            partitions.add((int) partition);
        }
        return List.copyOf(partitions);
    }

    private static int limit(@Nullable Object value) {
        if (value == null) {
            return DEFAULT_LIMIT;
        }
        long limit = integer(value, "limit");
        if (limit < 1 || limit > MAX_LIMIT) {
            throw new IllegalArgumentException("limit must be between 1 and " + MAX_LIMIT + ", was " + limit);
        }
        return (int) limit;
    }

    private static PayloadFormat format(@Nullable Object value) {
        if (value == null) {
            return PayloadFormat.AUTO;
        }
        if (value instanceof String name) {
            for (PayloadFormat format : PayloadFormat.values()) {
                if (format.label().equals(name.toLowerCase(Locale.ROOT))) {
                    return format;
                }
            }
        }
        throw new IllegalArgumentException("format must be one of " + PayloadFormat.labels() + ", was " + value);
    }

    private static @Nullable Long offset(@Nullable Object value, String name) {
        if (value == null) {
            return null;
        }
        long offset = integer(value, name);
        if (offset < 0) {
            throw new IllegalArgumentException(name + " must not be negative, was " + offset);
        }
        return offset;
    }

    private static @Nullable Map<Integer, Long> offsets(@Nullable Object value, String name) {
        if (value == null) {
            return null;
        }
        if (!(value instanceof Map<?, ?> map) || map.isEmpty()) {
            throw new IllegalArgumentException(name + " must be a non-empty object of partition number to offset");
        }
        Map<Integer, Long> offsets = new TreeMap<>();
        map.forEach((key, offset) -> {
            int partition;
            try {
                partition = Integer.parseInt(String.valueOf(key));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(name + " has the key '" + key + "', which is not a partition number");
            }
            if (partition < 0) {
                throw new IllegalArgumentException(name + " has the invalid partition " + partition);
            }
            offsets.put(partition, offset(offset, name + "." + partition));
        });
        return offsets;
    }

    private static @Nullable Instant time(@Nullable Object value, String name) {
        if (value == null) {
            return null;
        }
        if (value instanceof String text) {
            try {
                return OffsetDateTime.parse(text).toInstant();
            } catch (DateTimeParseException e) {
                // reported below
            }
        }
        throw new IllegalArgumentException(name + " must be an ISO-8601 date-time with an offset, such as "
                + "2026-11-25T10:00:00Z, was " + value);
    }

    private static boolean bool(@Nullable Object value, String name) {
        return switch (value) {
            case null -> false;
            case Boolean flag -> flag;
            default -> throw new IllegalArgumentException(name + " must be true or false");
        };
    }

    private static long integer(Object value, String name) {
        return switch (value) {
            case Integer number -> number;
            case Long number -> number;
            case Short number -> number;
            case BigInteger number when number.bitLength() < Long.SIZE -> number.longValue();
            case Double number when number == Math.rint(number) && Math.abs(number) < 1e15 -> number.longValue();
            default -> throw new IllegalArgumentException(name + " must be an integer, was " + value);
        };
    }
}

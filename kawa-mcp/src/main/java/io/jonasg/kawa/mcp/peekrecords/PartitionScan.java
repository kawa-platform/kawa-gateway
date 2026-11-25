package io.jonasg.kawa.mcp.peekrecords;

import io.jonasg.kawa.broker.RecordReader.FetchedRecord;
import io.jonasg.kawa.broker.RecordReader.LogRange;
import io.jonasg.kawa.broker.RecordReader.PartitionRead;
import io.jonasg.kawa.broker.RecordReader.Session;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/// One partition's part of an inspection: the offset range `[rangeStart, rangeEnd)` to inspect and how much of
/// it has been read, `[coveredFrom, coveredTo)`.
///
/// A scan with a start reads forward from `rangeStart`; one without reads the latest records, backwards from
/// `rangeEnd` in chunks. Either way the covered offsets stay one contiguous block, so whatever was not covered
/// is known not to have been inspected.
final class PartitionScan {

    final int partition;
    final @Nullable LogRange log;
    final @Nullable Long requestedStart;
    final @Nullable Long requestedEnd;
    final long rangeStart;
    final long rangeEnd;
    final boolean forward;
    final List<String> notes;
    final List<FetchedRecord> records = new ArrayList<>();
    long coveredFrom;
    long coveredTo;
    boolean timedOut;
    @Nullable String error;

    PartitionScan(int partition, @Nullable LogRange log, @Nullable Long requestedStart, @Nullable Long requestedEnd,
                  long rangeStart, long rangeEnd, boolean forward, List<String> notes) {
        this.partition = partition;
        this.log = log;
        this.requestedStart = requestedStart;
        this.requestedEnd = requestedEnd;
        this.rangeStart = rangeStart;
        this.rangeEnd = Math.max(rangeStart, rangeEnd);
        this.forward = forward;
        this.notes = notes;
        this.coveredFrom = forward ? rangeStart : this.rangeEnd;
        this.coveredTo = coveredFrom;
    }

    /// A partition that could not be inspected at all.
    static PartitionScan failed(int partition, String error) {
        var scan = new PartitionScan(partition, null, null, null, 0, 0, true, new ArrayList<>());
        scan.error = error;
        return scan;
    }

    /// The offsets of the range not yet covered: an upper bound on the records left, as offsets may have gaps.
    long uncovered() {
        return error != null || timedOut ? 0 : (rangeEnd - rangeStart) - (coveredTo - coveredFrom);
    }

    /// Whether every offset of the range was covered.
    boolean complete() {
        return error == null && coveredFrom == rangeStart && coveredTo == rangeEnd;
    }

    /// Reads up to `budget` more records within `timeout`, extending the covered block; returns how many were read.
    int read(Session session, String topic, int budget, Duration timeout) {
        try {
            if (forward) {
                PartitionRead read = session.read(topic, partition, coveredTo, rangeEnd, budget, timeout);
                records.addAll(read.records());
                timedOut = read.timedOut();
                // A read that stops short of its budget without timing out has reached the end of the range.
                coveredTo = !timedOut && read.records().size() < budget ? rangeEnd : read.nextOffset();
                return read.records().size();
            }
            // Backwards: the `budget` offsets below the covered block hold at most `budget` records.
            long from = Math.max(rangeStart, coveredFrom - budget);
            PartitionRead read = session.read(topic, partition, from, coveredFrom, budget, timeout);
            if (read.timedOut()) {
                // The chunk was only partly read; dropping it keeps the covered block contiguous.
                timedOut = true;
                notes.add("timed out reading offsets " + from + " to " + coveredFrom + ", which were not inspected");
                return 0;
            }
            records.addAll(0, read.records());
            coveredFrom = from;
            return read.records().size();
        } catch (RuntimeException e) {
            error = "could not read partition " + partition + ": " + message(e);
            return 0;
        }
    }

    static String message(Throwable error) {
        Throwable cause = error;
        while (cause.getCause() != null && cause.getMessage() == null) {
            cause = cause.getCause();
        }
        return cause.getMessage() == null ? cause.getClass().getSimpleName() : cause.getMessage();
    }

    /// Reads the scans until `limit` records were read in all, every range is covered, or `timeout` passes.
    /// Each round shares the records still allowed out evenly over the partitions that have offsets left, so a
    /// busy partition cannot crowd out the others; a partition with fewer offsets left than its share hands the
    /// rest to the others.
    ///
    /// @return whether the scan stopped at its timeout
    static boolean scanAll(Session session, String topic, List<PartitionScan> scans, int limit, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        int remaining = limit;
        while (remaining > 0) {
            var open = scans.stream().filter(scan -> scan.uncovered() > 0).toList();
            if (open.isEmpty()) {
                return false;
            }
            for (Map.Entry<PartitionScan, Integer> share : shares(open, remaining).entrySet()) {
                long left = deadline - System.nanoTime();
                if (left <= 0) {
                    return true;
                }
                remaining -= share.getKey().read(session, topic, share.getValue(), Duration.ofNanos(left));
                if (share.getKey().timedOut) {
                    return true;
                }
            }
        }
        return false;
    }

    /// Splits `budget` over `scans` in partition order, evenly, but never more than a scan's uncovered offsets.
    static Map<PartitionScan, Integer> shares(List<PartitionScan> scans, int budget) {
        Map<PartitionScan, Integer> shares = new LinkedHashMap<>();
        scans.forEach(scan -> shares.put(scan, 0));
        int left = budget;
        List<PartitionScan> hungry = scans;
        while (left > 0 && !hungry.isEmpty()) {
            int each = left / hungry.size();
            int extra = left % hungry.size();
            List<PartitionScan> stillHungry = new ArrayList<>();
            for (int i = 0; i < hungry.size(); i++) {
                var scan = hungry.get(i);
                int want = each + (i < extra ? 1 : 0);
                long room = scan.uncovered() - shares.get(scan);
                int give = (int) Math.min(want, room);
                shares.put(scan, shares.get(scan) + give);
                left -= give;
                if (room > give) {
                    stillHungry.add(scan);
                }
            }
            hungry = stillHungry;
        }
        shares.values().removeIf(share -> share == 0);
        return shares;
    }
}

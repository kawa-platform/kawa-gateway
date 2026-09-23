package io.jonasg.kawa.virtualtopic.filter;

import org.apache.kafka.common.record.internal.Record;

public interface RecordPredicate {
    boolean test(Record record);
}

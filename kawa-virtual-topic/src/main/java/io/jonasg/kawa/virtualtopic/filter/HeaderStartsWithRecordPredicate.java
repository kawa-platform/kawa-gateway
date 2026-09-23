package io.jonasg.kawa.virtualtopic.filter;

import io.jonasg.kawa.config.HeaderStartsWithFilterConfig;
import org.apache.kafka.common.record.internal.Record;

public class HeaderStartsWithRecordPredicate implements RecordPredicate {

    private final HeaderStartsWithFilterConfig config;

    public HeaderStartsWithRecordPredicate(HeaderStartsWithFilterConfig config) {
        this.config = config;
    }

    @Override
    public boolean test(Record record) {
        return HeaderValues.of(record, config.header()).stream()
                .anyMatch(value -> value.startsWith(config.value()));
    }
}

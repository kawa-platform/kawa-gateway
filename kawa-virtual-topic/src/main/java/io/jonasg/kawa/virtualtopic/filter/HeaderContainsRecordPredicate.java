package io.jonasg.kawa.virtualtopic.filter;

import io.jonasg.kawa.config.HeaderContainsFilterConfig;
import org.apache.kafka.common.record.internal.Record;

public class HeaderContainsRecordPredicate implements RecordPredicate {

    private final HeaderContainsFilterConfig config;

    public HeaderContainsRecordPredicate(HeaderContainsFilterConfig config) {
        this.config = config;
    }

    @Override
    public boolean test(Record record) {
        return HeaderValues.of(record, config.header()).stream()
                .anyMatch(value -> value.contains(config.value()));
    }
}

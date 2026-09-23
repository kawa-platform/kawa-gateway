package io.jonasg.kawa.virtualtopic.filter;

import io.jonasg.kawa.config.HeaderEqualsFilterConfig;
import org.apache.kafka.common.record.internal.Record;

public class HeaderEqualsRecordPredicate implements RecordPredicate {

    private final HeaderEqualsFilterConfig config;

    public HeaderEqualsRecordPredicate(HeaderEqualsFilterConfig config) {
        this.config = config;
    }

    @Override
    public boolean test(Record record) {
        return HeaderValues.of(record, config.header()).stream()
                .anyMatch(value -> config.value().equals(value));
    }
}

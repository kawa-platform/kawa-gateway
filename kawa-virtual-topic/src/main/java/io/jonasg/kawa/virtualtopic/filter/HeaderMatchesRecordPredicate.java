package io.jonasg.kawa.virtualtopic.filter;

import io.jonasg.kawa.config.HeaderMatchesFilterConfig;
import org.apache.kafka.common.record.internal.Record;

import java.util.regex.Pattern;

public class HeaderMatchesRecordPredicate implements RecordPredicate {

    private final String header;
    private final Pattern pattern;

    public HeaderMatchesRecordPredicate(HeaderMatchesFilterConfig config) {
        this.header = config.header();
        this.pattern = Pattern.compile(config.value());
    }

    @Override
    public boolean test(Record record) {
        return HeaderValues.of(record, header).stream()
                .anyMatch(value -> pattern.matcher(value).matches());
    }
}

package io.jonasg.kawa.virtualtopic.mapping;

import io.jonasg.kawa.config.DropHeadersTransformationConfig;
import io.jonasg.kawa.config.SetHeaderTransformationConfig;
import io.jonasg.kawa.config.TransformationConfig;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

/// Compiles the header steps of a virtual topic's transformations into one [HeaderMapping],
/// applied in order; value steps and `convert` are skipped (see [ValueMappings]).
///
/// | Step          | Effect                                                                 |
/// |---------------|------------------------------------------------------------------------|
/// | `dropHeaders` | removes every header with one of the names                             |
/// | `setHeader`   | replaces every header with the name by one header with the UTF-8 value |
///
/// Names match exactly, as Kafka header names are case-sensitive. Headers the steps do not name
/// keep their order; a set header goes last.
///
/// The mapping runs only on records whose value was transformed, so that the headers always
/// describe the delivered bytes: a record delivered as stored keeps its stored headers.
public final class HeaderMappings {

    private HeaderMappings() {
    }

    public static HeaderMapping compile(List<TransformationConfig> steps) {
        List<HeaderMapping> mappings = new ArrayList<>();
        for (TransformationConfig step : steps) {
            switch (step) {
                case DropHeadersTransformationConfig config -> mappings.add(drop(Set.copyOf(config.headers())));
                case SetHeaderTransformationConfig config -> mappings.add(set(
                        new RecordHeader(config.header(), config.value().getBytes(StandardCharsets.UTF_8))));
                default -> {
                }
            }
        }
        return switch (mappings.size()) {
            case 0 -> HeaderMapping.IDENTITY;
            case 1 -> mappings.getFirst();
            default -> headers -> {
                Header[] result = headers;
                for (HeaderMapping mapping : mappings) {
                    result = mapping.apply(result);
                }
                return result;
            };
        };
    }

    private static HeaderMapping drop(Set<String> names) {
        return headers -> Arrays.stream(headers)
                .filter(header -> !names.contains(header.key()))
                .toArray(Header[]::new);
    }

    private static HeaderMapping set(Header header) {
        return headers -> {
            List<Header> result = new ArrayList<>(headers.length + 1);
            for (Header existing : headers) {
                if (!existing.key().equals(header.key())) {
                    result.add(existing);
                }
            }
            result.add(header);
            return result.toArray(Header[]::new);
        };
    }
}

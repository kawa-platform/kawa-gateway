package io.jonasg.kawa.http;

import org.jspecify.annotations.NullUnmarked;

import java.util.List;
import java.util.Map;

/// A governance variable in a `PUT /governance/variables/{name}` body. Validated and converted
/// by [GovernanceConfigMapper].
///
/// @param name          optional; must equal the path name when present
/// @param type          `string`, `int`, `double`, `bool`, `list<string>` or `list<int>`
/// @param value         the literal in JSON syntax, e.g. `"[1, 4, 6, 12]"`
/// @param note          what the variable is for
/// @param samples       admin UI pattern-table hints; stored, never read by the gateway
/// @param notes         admin UI pattern-table hints; stored, never read by the gateway
/// @param extraExamples admin UI pattern-table hints; stored, never read by the gateway
@NullUnmarked
public record GovernanceVariableRequest(
        String name,
        String type,
        String value,
        String note,
        Map<String, String> samples,
        Map<String, String> notes,
        Map<String, List<String>> extraExamples
) {
}

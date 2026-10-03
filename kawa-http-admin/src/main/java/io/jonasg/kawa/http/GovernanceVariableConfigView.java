package io.jonasg.kawa.http;

import java.util.List;
import java.util.Map;

/// A governance variable in an admin `/governance/variables` response.
///
/// @param name          unique name
/// @param type          the CEL type, e.g. `list<int>`
/// @param value         the literal as authored, in JSON syntax
/// @param note          what the variable is for
/// @param samples       admin UI pattern-table hints
/// @param notes         admin UI pattern-table hints
/// @param extraExamples admin UI pattern-table hints
public record GovernanceVariableConfigView(
        String name,
        String type,
        String value,
        String note,
        Map<String, String> samples,
        Map<String, String> notes,
        Map<String, List<String>> extraExamples
) {
}

package io.jonasg.kawa.governance;

import java.util.List;

/// A single governance rule rejection: which rule rejected the request and why.
///
/// @param rule    the rule name
/// @param message the most specific error message: the failing sub-rule's own, else the
///                nearest group's, else the rule's
/// @param path    the sub-rule names leading to the failure, e.g. `[app, standard]`; empty
///                when the rule's `ANY` had no passing branch
public record Violation(String rule, String message, List<String> path) {

    public Violation(String rule, String message) {
        this(rule, message, List.of());
    }

    public Violation {
        path = path == null ? List.of() : List.copyOf(path);
    }

    /// The client-facing form, e.g. `[model > compaction > compact] model topics must be
    /// compacted`. A rule that is one check named after itself reads `[rule] message`.
    public String describe() {
        boolean selfNamed = path.size() == 1 && path.getFirst().equals(rule);
        String where = path.isEmpty() || selfNamed ? rule : rule + " > " + String.join(" > ", path);
        return "[" + where + "] " + message;
    }
}

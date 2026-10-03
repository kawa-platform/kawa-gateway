package io.jonasg.kawa.config;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

/// A named, typed constant that governance expressions can read, e.g. `partitionTiers` as
/// `list<int>` `[1, 4, 6, 12]`. Declared in every expression's CEL environment and bound at
/// evaluation time, never pasted into expression text.
///
/// [value] is the literal as authored, in JSON syntax: `"text"`, `3`, `2.5`, `true`,
/// `["a", "b"]`, `[1, 4]`. A `string` holding a regex may use named groups; they are only
/// display hints and are evaluated as plain groups.
///
/// The display fields ([samples], [notes], [extraExamples]) annotate a regex value for the
/// admin UI's pattern table; the gateway stores them and never reads them.
///
/// @param name          unique name, a valid CEL identifier
/// @param type          the CEL type expressions see
/// @param value         the literal, as JSON
/// @param note          what the variable is for
/// @param samples       pattern-table sample per named group; never `null`
/// @param notes         pattern-table note per displayed form; never `null`
/// @param extraExamples pattern-table extra examples per displayed form; never `null`
public record GovernanceVariableConfig(
        String name,
        Type type,
        String value,
        String note,
        Map<String, String> samples,
        Map<String, String> notes,
        Map<String, List<String>> extraExamples
) {

    public GovernanceVariableConfig(String name, Type type, String value, String note) {
        this(name, type, value, note, null, null, null);
    }

    public GovernanceVariableConfig {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("name must not be null or blank");
        }
        if (type == null) {
            throw new IllegalArgumentException("governance variable '" + name + "': type must not be null");
        }
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("governance variable '" + name + "': value must not be blank");
        }
        samples = samples == null ? Map.of() : Map.copyOf(samples);
        notes = notes == null ? Map.of() : Map.copyOf(notes);
        extraExamples = extraExamples == null ? Map.of() : Map.copyOf(extraExamples);
    }

    /// The CEL types a variable can have. The JSON name is the CEL spelling.
    public enum Type {
        @JsonProperty("string") STRING("string"),
        @JsonProperty("int") INT("int"),
        @JsonProperty("double") DOUBLE("double"),
        @JsonProperty("bool") BOOL("bool"),
        @JsonProperty("list<string>") LIST_STRING("list<string>"),
        @JsonProperty("list<int>") LIST_INT("list<int>");

        private final String celName;

        Type(String celName) {
            this.celName = celName;
        }

        /// The CEL spelling, e.g. `list<int>`.
        public String celName() {
            return celName;
        }
    }
}

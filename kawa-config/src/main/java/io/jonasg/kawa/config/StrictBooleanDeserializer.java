package io.jonasg.kawa.config;

import tools.jackson.core.JsonParser;
import tools.jackson.core.JsonToken;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.ValueDeserializer;

/// Reads a JSON `true` or `false` and refuses anything else. Jackson would coerce `"false"`, `0` or
/// `1` into a boolean, so a quoted or numeric value would configure something other than what it
/// reads as.
public final class StrictBooleanDeserializer extends ValueDeserializer<Boolean> {

    @Override
    public Boolean deserialize(JsonParser p, DeserializationContext ctxt) {
        JsonToken token = p.currentToken();
        if (token == JsonToken.VALUE_TRUE || token == JsonToken.VALUE_FALSE) {
            return token == JsonToken.VALUE_TRUE;
        }
        return (Boolean) ctxt.reportInputMismatch(Boolean.class, "expected true or false, got %s", p.getString());
    }
}

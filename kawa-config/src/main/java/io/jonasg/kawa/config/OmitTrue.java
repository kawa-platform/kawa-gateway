package io.jonasg.kawa.config;

/// A [com.fasterxml.jackson.annotation.JsonInclude.Include#CUSTOM] value filter that leaves a
/// `true` property out of the JSON: for an `errorHeader` setting, whose default is `true`, so only a
/// setting that turns it off is written, and configs that never mention it stay as they were.
public final class OmitTrue {

    @Override
    public boolean equals(Object value) {
        return Boolean.TRUE.equals(value);
    }

    @Override
    public int hashCode() {
        return 1;
    }
}

package com.example.lib;

/// Stands in for a library kawa ships. A plugin fixture bundles a different class with the same name to
/// prove that each side resolves its own version.
public final class Greeting {

    public String text() {
        return "kawa";
    }
}

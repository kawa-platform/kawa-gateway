package io.jonasg.kawa.config;

/// Thrown by a [ConfigTopicWriter] when another writer, typically another gateway replica, started a
/// config write since this one last did. The write was aborted and never becomes visible: the caller
/// must re-read the config topic and apply its change to the newer snapshot.
public final class ConfigWriteFencedException extends RuntimeException {

    public ConfigWriteFencedException(String message, Throwable cause) {
        super(message, cause);
    }
}

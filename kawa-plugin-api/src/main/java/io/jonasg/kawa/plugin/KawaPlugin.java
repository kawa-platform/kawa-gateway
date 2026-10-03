package io.jonasg.kawa.plugin;

/// Extension point for code that lives outside the kawa repository.
///
/// Implementations are discovered with [java.util.ServiceLoader] at gateway startup: a plugin JAR lists its
/// implementation class in `META-INF/services/io.jonasg.kawa.plugin.KawaPlugin` and is put on the gateway's
/// classpath (the Docker image adds every JAR in `/opt/kawa/plugins/`). Each plugin is instantiated through
/// its public no-arg constructor and [#register] is called once, before the gateway starts accepting
/// connections. An exception thrown from [#register] aborts startup.
public interface KawaPlugin {

    void register(KawaPluginContext context);
}

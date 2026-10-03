package io.jonasg.kawa.server.plugin;

import io.jonasg.kawa.plugin.KawaPlugin;
import io.jonasg.kawa.plugin.KawaPluginContext;

/// Test plugin registered through `src/test/resources/META-INF/services`; records the context it received.
public final class RecordingPlugin implements KawaPlugin {

    private KawaPluginContext context;

    @Override
    public void register(KawaPluginContext context) {
        this.context = context;
    }

    KawaPluginContext context() {
        return context;
    }
}

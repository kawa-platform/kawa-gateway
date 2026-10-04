---
title: Plugins
sidebar_position: 6
---

# Plugins

kawa can be extended with plugins: JARs that implement `io.jonasg.kawa.plugin.KawaPlugin` from the
`kawa-plugin-api` artifact and contribute through `KawaPluginContext`, for example an admin API
authentication provider (see [Writing an authentication provider](authentication.md#writing-an-authentication-provider)).
kawa's own `none` and `oauth` authentication providers are a plugin too.

## Installing a plugin

Plugins live in the plugin directory: `--plugins <dir>`, else the `KAWA_PLUGIN_DIR` environment variable, else
`/opt/kawa/plugins` (the directory in the Docker image).

```text
/opt/kawa/plugins/
├── kawa-pro/              one directory = one plugin, made of all JARs in it
│   ├── kawa-pro.jar       declares META-INF/services/io.jonasg.kawa.plugin.KawaPlugin
│   └── some-library.jar   the plugin's own dependencies, as plain JARs
└── small-plugin.jar       a self-contained plugin can also be a single JAR
```

A plugin that cannot be loaded — a missing class, a broken service entry, an exception while registering —
stops the gateway at startup with an error naming the plugin.

## Isolation

Every plugin gets its own classloader. A plugin sees:

- the JDK,
- `kawa-plugin-api` (`io.jonasg.kawa.plugin.*`) and slf4j (`org.slf4j.*`), shared with kawa,
- its own JARs.

It does **not** see kawa's internals or the libraries kawa itself bundles (Netty, Jackson, the Kafka client,
Nimbus, Logback, …). A plugin can therefore use any version of any library without clashing with kawa or with
other plugins, and cannot break when kawa upgrades its own dependencies.

When building a plugin:

- depend on `kawa-plugin-api` and `slf4j-api` with `provided` scope — kawa supplies them;
- ship every other dependency as a JAR in the plugin directory; no shading or relocation is needed;
- don't bundle an slf4j binding or Logback: plugin logs go through kawa's logging.

kawa calls into a plugin with the plugin's classloader as the thread's context classloader, so libraries that
look up classes or resources through it (`ServiceLoader`, Jackson modules, …) find the plugin's own.

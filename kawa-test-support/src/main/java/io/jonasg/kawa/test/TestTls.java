package io.jonasg.kawa.test;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/// The test-only TLS fixtures in `src/main/resources/tls` (see `generate.sh` there): a CA, a broker
/// certificate for `localhost` / `127.0.0.1` and a gateway client certificate, as PKCS12, JKS and
/// PEM. Every store and key uses [#PASSWORD].
///
/// Fixtures are copied to real files, since TLS settings take file paths and the fixtures may sit
/// inside this module's jar.
public final class TestTls {

    /// The password of every fixture store and encrypted key.
    public static final String PASSWORD = "changeit";

    private TestTls() {
    }

    /// A fixture, such as `truststore.p12` or `gateway.pem`, as a file.
    public static Path file(String name) {
        try (InputStream in = TestTls.class.getResourceAsStream("/tls/" + name)) {
            if (in == null) {
                throw new IllegalArgumentException("no TLS fixture '" + name + "'");
            }
            Path file = Files.createTempFile("kawa-tls-", "-" + name);
            file.toFile().deleteOnExit();
            Files.copy(in, file, StandardCopyOption.REPLACE_EXISTING);
            return file;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}

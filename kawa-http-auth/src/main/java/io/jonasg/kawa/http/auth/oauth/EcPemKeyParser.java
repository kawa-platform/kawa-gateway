package io.jonasg.kawa.http.auth.oauth;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSObject;
import com.nimbusds.jose.Payload;
import com.nimbusds.jose.crypto.ECDSASigner;
import com.nimbusds.jose.crypto.ECDSAVerifier;
import com.nimbusds.jose.jwk.Curve;
import com.nimbusds.jose.jwk.ECKey;
import com.nimbusds.jose.util.Base64URL;

import java.util.Arrays;
import java.util.Base64;

/// Parses a PEM-encoded EC P-256 private key into a Nimbus [ECKey] using only the JDK.
///
/// Nimbus' own PEM support needs BouncyCastle, which kawa does not ship. Both forms produced by
/// `openssl` are accepted: PKCS#8 (`BEGIN PRIVATE KEY`, e.g. `openssl genpkey -algorithm EC
/// -pkeyopt ec_paramgen_curve:P-256`) and SEC1 (`BEGIN EC PRIVATE KEY`, e.g. `openssl ecparam
/// -name prime256v1 -genkey -noout`). Both embed an RFC 5915 `ECPrivateKey`, whose optional public
/// key must be present. The parsed pair is checked by signing and verifying a probe.
final class EcPemKeyParser {

    private static final int SEQUENCE = 0x30;
    private static final int INTEGER = 0x02;
    private static final int OCTET_STRING = 0x04;
    private static final int BIT_STRING = 0x03;
    private static final int PUBLIC_KEY_TAG = 0xA1;
    private static final int P256_FIELD_BYTES = 32;

    private EcPemKeyParser() {
    }

    static ECKey parse(String pem) {
        String type = pemType(pem);
        byte[] der = Base64.getMimeDecoder().decode(pem
                .replaceAll("-----(BEGIN|END) [A-Z ]+-----", "")
                .replaceAll("\\s", ""));
        byte[] ecPrivateKey = switch (type) {
            case "PRIVATE KEY" -> unwrapPkcs8(der);
            case "EC PRIVATE KEY" -> der;
            default -> throw invalid("unsupported PEM type '" + type + "'");
        };
        ECKey key = fromEcPrivateKey(ecPrivateKey);
        checkPair(key);
        return key;
    }

    /// `PrivateKeyInfo ::= SEQUENCE { version INTEGER, algorithm AlgorithmIdentifier, privateKey OCTET STRING }`
    private static byte[] unwrapPkcs8(byte[] der) {
        var info = new Der(new Der(der).read(SEQUENCE));
        info.read(INTEGER);
        info.read(SEQUENCE);
        return info.read(OCTET_STRING);
    }

    /// `ECPrivateKey ::= SEQUENCE { version INTEGER, privateKey OCTET STRING, [0] parameters OPTIONAL,
    /// [1] publicKey BIT STRING OPTIONAL }`
    private static ECKey fromEcPrivateKey(byte[] der) {
        var key = new Der(new Der(der).read(SEQUENCE));
        key.read(INTEGER);
        byte[] d = key.read(OCTET_STRING);
        if (d.length != P256_FIELD_BYTES) {
            throw invalid("not a P-256 key");
        }
        byte[] point = null;
        while (key.hasMore()) {
            int tag = key.peekTag();
            byte[] content = key.read(tag);
            if (tag == PUBLIC_KEY_TAG) {
                point = new Der(content).read(BIT_STRING);
            }
        }
        // BIT STRING: one "unused bits" byte (0), then an uncompressed point 0x04 || X || Y
        if (point == null || point.length != 2 + 2 * P256_FIELD_BYTES || point[0] != 0 || point[1] != 0x04) {
            throw invalid("the key does not embed its public key; generate it with openssl genpkey or openssl ecparam");
        }
        byte[] x = Arrays.copyOfRange(point, 2, 2 + P256_FIELD_BYTES);
        byte[] y = Arrays.copyOfRange(point, 2 + P256_FIELD_BYTES, point.length);
        try {
            return new ECKey.Builder(Curve.P_256, Base64URL.encode(x), Base64URL.encode(y))
                    .d(Base64URL.encode(d))
                    .build();
        } catch (IllegalStateException | IllegalArgumentException e) {
            throw invalid("not a valid P-256 key");
        }
    }

    private static void checkPair(ECKey key) {
        try {
            var probe = new JWSObject(new JWSHeader(JWSAlgorithm.ES256), new Payload("kawa"));
            probe.sign(new ECDSASigner(key));
            if (!probe.verify(new ECDSAVerifier(key.toPublicJWK()))) {
                throw invalid("the private and public key do not belong together");
            }
        } catch (JOSEException e) {
            throw invalid("not a usable P-256 key");
        }
    }

    private static String pemType(String pem) {
        int begin = pem.indexOf("-----BEGIN ");
        int end = begin < 0 ? -1 : pem.indexOf("-----", begin + 11);
        if (begin < 0 || end < 0) {
            throw invalid("missing PEM header");
        }
        return pem.substring(begin + 11, end);
    }

    private static IllegalArgumentException invalid(String reason) {
        return new IllegalArgumentException("admin.auth.settings.signingKey: " + reason);
    }

    /// A minimal reader over a sequence of DER TLV elements.
    private static final class Der {

        private final byte[] bytes;
        private int position;

        Der(byte[] bytes) {
            this.bytes = bytes;
        }

        boolean hasMore() {
            return position < bytes.length;
        }

        int peekTag() {
            return bytes[position] & 0xFF;
        }

        /// Reads the next element, which must have tag `expectedTag`, and returns its content.
        byte[] read(int expectedTag) {
            if (!hasMore() || peekTag() != expectedTag) {
                throw invalid("malformed key structure");
            }
            position++;
            int length = bytes[position++] & 0xFF;
            if (length > 0x7F) {
                int lengthBytes = length & 0x7F;
                if (lengthBytes > 3) {
                    throw invalid("malformed key structure");
                }
                length = 0;
                for (int i = 0; i < lengthBytes; i++) {
                    length = (length << 8) | (bytes[position++] & 0xFF);
                }
            }
            if (position + length > bytes.length) {
                throw invalid("malformed key structure");
            }
            byte[] content = Arrays.copyOfRange(bytes, position, position + length);
            position += length;
            return content;
        }
    }
}

package io.jonasg.kawa.test;

/// PEM fixtures for signing key tests, generated with LibreSSL/OpenSSL. Test-only keys.
public final class TestSigningKeys {

    /// `openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256` (LibreSSL: explicit curve parameters).
    public static final String PKCS8_P256 = """
            -----BEGIN PRIVATE KEY-----
            MIIBeQIBADCCAQMGByqGSM49AgEwgfcCAQEwLAYHKoZIzj0BAQIhAP////8AAAAB
            AAAAAAAAAAAAAAAA////////////////MFsEIP////8AAAABAAAAAAAAAAAAAAAA
            ///////////////8BCBaxjXYqjqT57PrvVV2mIa8ZR0GsMxTsPY7zjw+J9JgSwMV
            AMSdNgiG5wSTamZ44ROdJreBn36QBEEEaxfR8uEsQkf4vOblY6RA8ncDfYEt6zOg
            9KE5RdiYwpZP40Li/hp/m47n60p8D54WK84zV2sxXs7LtkBoN79R9QIhAP////8A
            AAAA//////////+85vqtpxeehPO5ysL8YyVRAgEBBG0wawIBAQQgjntiR7zIs5FR
            jxi5PAQF4FWQsY7rllkr3PUsmPt89lChRANCAASlsbYjJC1iWB6Sw0lWkbXuf3jf
            56Vp/ZtqBuuZcJi35+gK3c3cl7x/fi/BhgvyB+wzwEb3q+CuhGajDcFz/MBW
            -----END PRIVATE KEY-----
            """;

    /// `openssl ecparam -name prime256v1 -genkey -noout`.
    public static final String SEC1_P256 = """
            -----BEGIN EC PRIVATE KEY-----
            MHcCAQEEIBBG1rvWVXUh15loXA3KHUF4yG+Et7ewOBBXYunlCKk3oAoGCCqGSM49
            AwEHoUQDQgAEFkFXdmcLPuv+ompK3KY49qNHRSYJXYIMZI2PhK/H42u40uQOfJ85
            +7OvqMWFH72JrbXBty5OCL24y24F20P10g==
            -----END EC PRIVATE KEY-----
            """;

    /// [#SEC1_P256] with the optional public key removed.
    public static final String SEC1_P256_WITHOUT_PUBLIC_KEY = """
            -----BEGIN EC PRIVATE KEY-----
            MDECAQEEIBBG1rvWVXUh15loXA3KHUF4yG+Et7ewOBBXYunlCKk3oAoGCCqGSM49AwEH
            -----END EC PRIVATE KEY-----
            """;

    /// `openssl ecparam -name secp384r1 -genkey -noout`.
    public static final String SEC1_P384 = """
            -----BEGIN EC PRIVATE KEY-----
            MIGkAgEBBDAm5UXHRoZze25aDC01A92Wva32WN0sQpQVdWjv2iQF9tal8WMpJOP3
            zGRAh6F4UESgBwYFK4EEACKhZANiAASjbvvgwCnqbbBhh8dS+iNhkeURZWAjEQ50
            MQj3NzVb8xEh+cie3k+IhzetN9I+mW6AZ/sQRm/pO351Z8tV9U6O3mTGZMbP8p0e
            w7qMDBnGdwyMKl7ZxTqhfpvrZnz8eQk=
            -----END EC PRIVATE KEY-----
            """;

    private TestSigningKeys() {
    }
}

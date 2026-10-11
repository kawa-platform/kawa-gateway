#!/usr/bin/env bash
# Regenerates the TEST-ONLY TLS fixtures in this directory: a CA, a broker certificate (SAN
# localhost / 127.0.0.1) and a gateway client certificate, each as PKCS12, JKS and PEM. Never use
# them outside tests. other-ca.pem is an unrelated CA, for tests that must not trust the broker. Every store and key is protected with the password "changeit".
#
# Needs keytool (Java 21+) and openssl on the PATH. Run from this directory: ./generate.sh
set -euo pipefail

PASS=changeit
DAYS=36500
cd "$(dirname "$0")"
rm -f ./*.p12 ./*.jks ./*.pem ./*.crt ./*.csr ./*.key

# CA
keytool -genkeypair -alias ca -dname "CN=kawa-test-ca" -ext bc:c -keyalg RSA -keysize 2048 -validity $DAYS \
  -storetype PKCS12 -keystore ca.p12 -storepass $PASS
keytool -exportcert -rfc -alias ca -keystore ca.p12 -storepass $PASS -file ca.pem

# truststores holding the CA
keytool -importcert -noprompt -alias ca -file ca.pem -storetype PKCS12 -keystore truststore.p12 -storepass $PASS
keytool -importcert -noprompt -alias ca -file ca.pem -storetype JKS -keystore truststore.jks -storepass $PASS

# a key pair signed by the CA, as <name>.p12, <name>.jks, <name>.pem (key + chain) and <name>-encrypted.pem
issue() {
  local name=$1 ext=$2
  keytool -genkeypair -alias "$name" -dname "CN=$name" -keyalg RSA -keysize 2048 -validity $DAYS \
    -storetype PKCS12 -keystore "$name.p12" -storepass $PASS
  keytool -certreq -alias "$name" -keystore "$name.p12" -storepass $PASS -file "$name.csr"
  keytool -gencert -alias ca -keystore ca.p12 -storepass $PASS -infile "$name.csr" -outfile "$name.crt" -rfc \
    -validity $DAYS -ext KU=digitalSignature,keyEncipherment -ext EKU=serverAuth,clientAuth $ext
  keytool -importcert -noprompt -alias ca -file ca.pem -keystore "$name.p12" -storepass $PASS
  keytool -importcert -alias "$name" -file "$name.crt" -keystore "$name.p12" -storepass $PASS
  keytool -importkeystore -noprompt -srckeystore "$name.p12" -srcstoretype PKCS12 -srcstorepass $PASS \
    -destkeystore "$name.jks" -deststoretype JKS -deststorepass $PASS
  openssl pkcs12 -in "$name.p12" -nocerts -nodes -passin pass:$PASS | openssl pkcs8 -topk8 -nocrypt > "$name.key"
  cat "$name.key" "$name.crt" ca.pem > "$name.pem"
  openssl pkcs8 -topk8 -v2 aes-256-cbc -in "$name.key" -passout pass:$PASS > "$name-encrypted.key"
  cat "$name-encrypted.key" "$name.crt" ca.pem > "$name-encrypted.pem"
  rm -f "$name.csr" "$name.crt" "$name.key" "$name-encrypted.key"
}

issue broker "-ext SAN=dns:localhost,ip:127.0.0.1"
issue gateway ""
rm -f ca.p12   # the CA's private key is not needed by any test

# an unrelated CA, for tests that must not trust the broker
keytool -genkeypair -alias other -dname "CN=kawa-test-other-ca" -ext bc:c -keyalg RSA -keysize 2048 -validity $DAYS \
  -storetype PKCS12 -keystore other-ca.p12 -storepass $PASS
keytool -exportcert -rfc -alias other -keystore other-ca.p12 -storepass $PASS -file other-ca.pem
rm -f other-ca.p12

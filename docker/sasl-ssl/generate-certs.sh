#!/usr/bin/env sh
# Generates the DEMO-ONLY certificates for docker-compose.sasl-ssl.yml into the directory given as
# the first argument (default ./certs): a throwaway CA, a certificate for the Kafka broker and one
# for the kawa gateway, as PKCS12 stores. Every store uses the password in $PASS (default
# "changeit"). Never use these outside the demo.
#
# Runs in the `certs` service of the compose file (any image with keytool), and skips the work when
# the certificates already exist, so they survive `docker compose down` / `up`. Delete the directory
# to regenerate them.
set -eu

OUT=${1:-./certs}
PASS=${PASS:-changeit}
DAYS=3650

mkdir -p "$OUT"
cd "$OUT"
if [ -f truststore.p12 ] && [ -f kafka.p12 ] && [ -f gateway.p12 ]; then
  echo "certificates already present in $OUT"
  exit 0
fi
rm -f ./*.p12 ./*.pem ./*.crt ./*.csr credentials

# CA, and a truststore holding it for every party: broker, gateway and clients
keytool -genkeypair -alias ca -dname "CN=kawa-demo-ca" -ext bc:c -keyalg RSA -keysize 2048 -validity $DAYS \
  -storetype PKCS12 -keystore ca.p12 -storepass "$PASS"
keytool -exportcert -rfc -alias ca -keystore ca.p12 -storepass "$PASS" -file ca.pem
keytool -importcert -noprompt -alias ca -file ca.pem -storetype PKCS12 -keystore truststore.p12 -storepass "$PASS"

# a key pair signed by the CA, as <name>.p12, for the host names it is reached on
issue() {
  name=$1
  san=$2
  keytool -genkeypair -alias "$name" -dname "CN=$name" -keyalg RSA -keysize 2048 -validity $DAYS \
    -storetype PKCS12 -keystore "$name.p12" -storepass "$PASS"
  keytool -certreq -alias "$name" -keystore "$name.p12" -storepass "$PASS" -file "$name.csr"
  keytool -gencert -alias ca -keystore ca.p12 -storepass "$PASS" -infile "$name.csr" -outfile "$name.crt" -rfc \
    -validity $DAYS -ext KU=digitalSignature,keyEncipherment -ext EKU=serverAuth,clientAuth -ext "SAN=$san"
  keytool -importcert -noprompt -alias ca -file ca.pem -keystore "$name.p12" -storepass "$PASS"
  keytool -importcert -alias "$name" -file "$name.crt" -keystore "$name.p12" -storepass "$PASS"
  rm -f "$name.csr" "$name.crt"
}

# the broker is reached as kafka inside the compose network and as localhost from the host
issue kafka "dns:kafka,dns:localhost"
# clients reach the gateway on its advertised host, localhost
issue gateway "dns:localhost,dns:gateway"
rm -f ca.p12   # the CA's private key is not needed once everything is signed

# the store password, in the file the Kafka image reads it from
printf '%s' "$PASS" > credentials

# a ready-made client config for the Kafka CLI tools (paths as mounted in the kafka-cli service)
cat > client.properties <<PROPS
security.protocol=SASL_SSL
ssl.truststore.location=/etc/kawa/certs/truststore.p12
ssl.truststore.password=$PASS
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="alice" password="alice-secret";
PROPS

# the Kafka and gateway containers run as non-root users
chmod 644 ./*
echo "certificates written to $OUT"

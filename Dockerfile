# kawa - Kafka access gateway (virtual topics)
#
# Build:   docker build -t kawa-gateway .
# Run:     docker run -p 9092:9092 -v $(pwd)/gateway.yaml:/etc/kawa/config.yaml:ro kawa-gateway
#
# The gateway is plaintext Kafka protocol only (Milestone 1). Clients connect to the
# published port; make sure the `advertised` section of the config YAML resolves from
# the CLIENT's perspective (host/port as reachable by clients), not from inside the
# container. See docker-compose.yml + docker/gateway.yaml for a working example.

# ---- build stage ------------------------------------------------------------
FROM eclipse-temurin:26-jdk-jammy AS build

WORKDIR /workspace

# Cache the Maven wrapper and dependencies separately from sources so code changes
# don't re-download the world.
COPY mvnw ./
COPY .mvn .mvn
COPY pom.xml ./
COPY checkstyle.xml ./
COPY kawa-config/pom.xml kawa-config/
COPY kawa-test-support/pom.xml kawa-test-support/
COPY kawa-core/pom.xml kawa-core/
COPY kawa-protocol-kafka/pom.xml kawa-protocol-kafka/
COPY kawa-virtual-topic/pom.xml kawa-virtual-topic/
COPY kawa-server/pom.xml kawa-server/
COPY kawa-rbac/pom.xml kawa-rbac/
COPY kawa-governance/pom.xml kawa-governance/
COPY kawa-http-admin/pom.xml kawa-http-admin/
COPY kawa-integration-tests/pom.xml kawa-integration-tests/

RUN --mount=type=cache,target=/root/.m2 \
    ./mvnw -B -pl kawa-server -am dependency:go-offline || true

COPY kawa-config/src kawa-config/src
COPY kawa-test-support/src kawa-test-support/src
COPY kawa-core/src kawa-core/src
COPY kawa-protocol-kafka/src kawa-protocol-kafka/src
COPY kawa-virtual-topic/src kawa-virtual-topic/src
COPY kawa-server/src kawa-server/src
COPY kawa-rbac/src kawa-rbac/src
COPY kawa-governance/src kawa-governance/src
COPY kawa-http-admin/src kawa-http-admin/src
COPY kawa-integration-tests/src kawa-integration-tests/src

RUN --mount=type=cache,target=/root/.m2 \
    ./mvnw -B clean package -DskipTests -pl kawa-server -am

# ---- runtime stage ----------------------------------------------------------
FROM eclipse-temurin:26-jre-alpine AS runtime

RUN addgroup -S kawa && adduser -S kawa -G kawa

WORKDIR /app
COPY --from=build /workspace/kawa-server/target/kawa-server-*.jar /app/gateway.jar

USER kawa
EXPOSE 9092
EXPOSE 9092

# Config is expected at /etc/kawa/config.yaml (mount it read-only).
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "/app/gateway.jar", "--config", "/etc/kawa/config.yaml"]

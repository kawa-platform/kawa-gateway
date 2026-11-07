# kawa - Kafka access gateway
#
# One-command Docker Compose workflows. `make up` ALWAYS rebuilds the gateway image
# and force-recreates containers, so code and config changes are picked up reliably.
# A plain `docker compose up` can silently keep running a stale image/container.

COMPOSE      := docker compose
COMPOSE_SASL := docker compose -f docker-compose.sasl.yml
COMPOSE_SASL_SSL := docker compose -f docker-compose.sasl-ssl.yml
DEBUG_JAVA   := java -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005 -jar kawa-server/target/kawa-server-*-SNAPSHOT.jar

.PHONY: help up up-sasl up-sasl-ssl debug debug-noauth down down-sasl down-sasl-ssl logs logs-sasl logs-sasl-ssl ps build test clean release

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

up: build ## Build + start the plain demo stack (always rebuilds image and recreates containers)
	$(COMPOSE) up -d --build --force-recreate

up-sasl: ## Build + start the SASL demo stack (always rebuilds image and recreates containers)
	$(COMPOSE_SASL) up -d --build --force-recreate

up-sasl-ssl: ## Build + start the SASL_SSL demo stack (TLS + SASL on every hop; generates demo certificates)
	$(COMPOSE_SASL_SSL) up -d --build --force-recreate

# The gateway runs on the host with a JDWP agent on :5005 (attach with the "Kawa (attach 5005)" IntelliJ run config).
# Kafka stays in Docker and is reached via its published EXTERNAL listener. Stop Kafka with `make down`.

# Same config as `make up` (oauth, mcp module off); only the bootstrap server differs. See docker/gateway-debug.yaml.
debug: build ## Start Kafka in Docker, run the gateway on the host with a debug agent on :5005 (same config as `make up`)
	$(COMPOSE) up -d --force-recreate kafka kafka-init
	$(DEBUG_JAVA) --config docker/gateway-debug.yaml

# Like `debug`, but with an open admin API (auth.type none) and the mcp module enabled, so `curl` works without a token.
debug-noauth: build ## Like debug, but no admin auth and the mcp module enabled (curl-friendly)
	$(COMPOSE) up -d --force-recreate kafka kafka-init
	@# enable the mcp module once the admin API is up (the java process below blocks; gives up after 60s)
	@( for i in $$(seq 60); do \
		curl -sf -o /dev/null localhost:8080/modules && \
		curl -sf -X PUT localhost:8080/modules/mcp -H 'Content-Type: application/json' -d '{"enabled":true}' -o /dev/null && \
		echo "mcp module enabled" && break; \
		sleep 1; done ) &
	$(DEBUG_JAVA) --config docker/gateway-debug-noauth.yaml

down: ## Stop and remove the plain stack
	$(COMPOSE) down

down-sasl: ## Stop and remove the SASL stack
	$(COMPOSE_SASL) down

down-sasl-ssl: ## Stop and remove the SASL_SSL stack
	$(COMPOSE_SASL_SSL) down

logs: ## Follow logs of the plain stack
	$(COMPOSE) logs -f

logs-sasl: ## Follow logs of the SASL stack
	$(COMPOSE_SASL) logs -f

logs-sasl-ssl: ## Follow logs of the SASL_SSL stack
	$(COMPOSE_SASL_SSL) logs -f

ps: ## Show status of the plain stack
	$(COMPOSE) ps

build: ## Build the gateway jar locally (no Docker)
	./mvnw -pl kawa-server -am package -DskipTests

test: ## Run all unit tests
	./mvnw test

clean: ## Clean all build outputs
	./mvnw clean

release: ## Tag v$(VERSION) on origin/main and push it (starts the Release workflow). VERSION=x.y.z[-rc.N]
	@./scripts/release.sh '$(VERSION)'

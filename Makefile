# kawa - Kafka access gateway
#
# One-command Docker Compose workflows. `make up` ALWAYS rebuilds the gateway image
# and force-recreates containers, so code and config changes are picked up reliably.
# A plain `docker compose up` can silently keep running a stale image/container.

COMPOSE      := docker compose
COMPOSE_SASL := docker compose -f docker-compose.sasl.yml
COMPOSE_SASL_SSL := docker compose -f docker-compose.sasl-ssl.yml

.PHONY: help up up-sasl up-sasl-ssl down down-sasl down-sasl-ssl logs logs-sasl logs-sasl-ssl ps build test clean release

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

up: build ## Build + start the plain demo stack (always rebuilds image and recreates containers)
	$(COMPOSE) up -d --build --force-recreate

up-sasl: ## Build + start the SASL demo stack (always rebuilds image and recreates containers)
	$(COMPOSE_SASL) up -d --build --force-recreate

up-sasl-ssl: ## Build + start the SASL_SSL demo stack (TLS + SASL on every hop; generates demo certificates)
	$(COMPOSE_SASL_SSL) up -d --build --force-recreate

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

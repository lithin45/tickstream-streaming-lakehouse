# TickStream — one-command developer workflow.
# Targets: install up down test test-unit demo record replay lint format clean

export COMPOSE_PROJECT_NAME := tickstream
COMPOSE := docker compose
UV := uv

.DEFAULT_GOAL := help
.PHONY: help install up down ps logs console test test-unit demo demo-container record replay produce lint format format-check clean

help: ## Show this help.
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

install: ## Create the venv and install core + dev dependencies.
	$(UV) sync

up: ## Start Redpanda (+ Console) and wait until healthy.
	$(COMPOSE) up -d --wait redpanda console
	@echo "Redpanda ready on localhost:19092 | Console: http://localhost:8080"

down: ## Stop the stack (keeps volumes).
	$(COMPOSE) down

ps: ## Show running services.
	$(COMPOSE) ps

logs: ## Tail Redpanda logs.
	$(COMPOSE) logs -f redpanda

console: ## Open the Redpanda Console URL.
	@echo "http://localhost:8080"

test: up install ## Run the full test suite against a live broker.
	$(UV) run pytest

test-unit: install ## Run only unit tests (no broker required).
	$(UV) run pytest -m "not integration"

demo: up install ## Phase 1 demo (host): publish hand-crafted events and read them back, exact.
	$(UV) run tickstream demo

demo-container: up ## Phase 1 demo in Docker: build the image and run the round-trip in-container.
	$(COMPOSE) --profile demo run --rm --build demo

RECORD_SECONDS ?= 60

record: install ## Record a short LIVE stream to fixtures/recorded_stream.jsonl (needs network).
	$(UV) run tickstream record --seconds $(RECORD_SECONDS)

replay: up install ## Replay the committed fixture through Redpanda (offline, deterministic).
	$(UV) run tickstream replay

produce: up install ## Run the LIVE producer (exchange WebSocket -> Redpanda). Ctrl-C to stop.
	$(UV) run tickstream produce

lint: install ## Lint with ruff.
	$(UV) run ruff check .

format: install ## Auto-format with ruff.
	$(UV) run ruff format .

format-check: install ## Check formatting without writing.
	$(UV) run ruff format --check .

clean: ## Remove generated data and caches.
	rm -rf lake_data warehouse .pytest_cache .ruff_cache **/__pycache__ *.duckdb

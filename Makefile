# TickStream — one-command developer workflow.
# Targets: install up down test demo record replay process bronze pipeline lint format

export COMPOSE_PROJECT_NAME := tickstream
COMPOSE := docker compose
UV := uv
# All Python deps (incl. processing/lake/dbt/quality/ui extras) so every layer is importable.
URUN := $(UV) run --all-extras

.DEFAULT_GOAL := help
.PHONY: help install up down ps logs console test test-unit demo demo-container \
        record replay produce process bronze marts query contracts pipeline lint format format-check clean

help: ## Show this help.
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

install: ## Create the venv and install all deps (core + extras + dev).
	$(UV) sync --all-extras

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
	$(URUN) pytest

test-unit: install ## Run only unit tests (no broker required).
	$(URUN) pytest -m "not integration"

demo: up install ## Phase 1 demo (host): publish hand-crafted events and read them back, exact.
	$(URUN) tickstream demo

demo-container: up ## Phase 1 demo in Docker: build the image and run the round-trip in-container.
	$(COMPOSE) --profile demo run --rm --build demo

RECORD_SECONDS ?= 60

record: install ## Record a short LIVE stream to fixtures/recorded_stream.jsonl (needs network).
	$(URUN) tickstream record --seconds $(RECORD_SECONDS)

replay: up install ## Reproduce the whole pipeline offline: replay -> bronze -> windows (deterministic).
	$(URUN) tickstream pipeline

process: up install ## Run the Quix Streams windowed processor (live; Ctrl-C to stop).
	$(URUN) tickstream process

bronze: up install ## Drain raw topics into bronze Parquet.
	$(URUN) tickstream bronze

marts: install ## Build dbt silver/gold marts + land gold in Apache Iceberg (reads bronze).
	$(URUN) tickstream build-marts

query: install ## Example DuckDB SQL over gold Iceberg + an Iceberg time-travel query.
	$(URUN) tickstream query

contracts: install ## Validate the landed bronze against the data contract (quarantine count).
	$(URUN) tickstream contracts

pipeline: replay ## Alias for `make replay` (full offline pipeline).

produce: up install ## Run the LIVE producer (exchange WebSocket -> Redpanda). Ctrl-C to stop.
	$(URUN) tickstream produce

lint: install ## Lint with ruff.
	$(URUN) ruff check .

format: install ## Auto-format with ruff.
	$(URUN) ruff format .

format-check: install ## Check formatting without writing.
	$(URUN) ruff format --check .

clean: ## Remove generated data and caches.
	rm -rf lake_data warehouse .pytest_cache .ruff_cache **/__pycache__ *.duckdb

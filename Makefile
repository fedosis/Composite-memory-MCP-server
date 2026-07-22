.PHONY: install test lint all migrate build install-plugin test-plugin

# Standard targets
install:
	pip install -e ".[dev]"

test:
	pytest tests/ -q

lint:
	ruff check src/

all: lint test

migrate:
	alembic upgrade head

build:
	python3 -m build

# Plugin targets (Hermes v0.19 CMMS user-plugin shim)

# Deploy the Hermes user-plugin shim to ~/.hermes/plugins/memory_server/
install-plugin:
	bash scripts/install-plugin.sh

# Run all plugin-related tests
test-plugin:
	python3 -m pytest tests/test_hermes_plugin_registration.py -v

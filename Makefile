.PHONY: install test lint all migrate build install-plugin test-plugin schema-artifacts test-native

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

# Native transport targets (AF_UNIX owner/service/client)

# Regenerate the canonical schema artifacts shipped with the plugin
schema-artifacts:
	python -m memory_server.native.gen_schema --out plugin-dist/memory_server_proxy

# Run the native transport and proxy plugin test suites
test-native:
	python -m pytest tests/test_native_*.py tests/test_proxy_*.py -q

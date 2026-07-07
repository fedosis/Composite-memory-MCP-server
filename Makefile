.PHONY: install test lint all migrate build

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

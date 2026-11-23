# Convenience targets. Override variables on the command line, e.g.
#   make deploy ENV=qa REGION=us-west-2 PREFIX=dp
ENV ?= dev
REGION ?= us-west-2
PREFIX ?= dp
ACCOUNT ?= $(shell aws sts get-caller-identity --query Account --output text 2>/dev/null)
MIGRATIONS ?= migrations
PYTHON ?= python3
VENV ?= .venv
BIN := $(VENV)/bin

.PHONY: help install test lint synth deploy apply archive clean

help:
	@echo "install          create $(VENV) and install the toolkit with dev extras"
	@echo "test             run the test suite"
	@echo "lint             ruff check"
	@echo "synth / deploy   cdk synth / deploy the stack for ENV"
	@echo "apply            run schema-evolution-apply against ENV"
	@echo "archive          zip the committed files (no cdk.out, .venv or local files) to dist/"

install:
	$(PYTHON) -m venv $(VENV)
	$(BIN)/pip install --upgrade pip
	$(BIN)/pip install -e ".[dev]"

test:
	$(BIN)/pytest

lint:
	$(BIN)/ruff check .

synth:
	cd infra && PATH="$(abspath $(BIN)):$$PATH" npx -y aws-cdk@latest synth -c env=$(ENV) -c region=$(REGION) -c prefix=$(PREFIX) 

deploy:
	cd infra && PATH="$(abspath $(BIN)):$$PATH" npx -y aws-cdk@latest deploy -c env=$(ENV) -c region=$(REGION) -c prefix=$(PREFIX) 

apply:
	$(BIN)/schema-evolution-apply --region $(REGION) --account_id $(ACCOUNT) --environment $(ENV) \
		--bucket_prefix $(PREFIX) --migrations_dir $(MIGRATIONS)/

archive:
	mkdir -p dist
	git archive --format=zip --prefix=schema-evolution-toolkit/ -o dist/schema-evolution-toolkit.zip HEAD
	@echo "Wrote dist/schema-evolution-toolkit.zip"
clean:
	rm -rf dist infra/cdk.out .pytest_cache
	find . -name __pycache__ -type d -prune -exec rm -rf {} +

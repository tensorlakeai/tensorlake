
all: build

build:
	@rm -rf dist
	@poetry install
	@poetry build

fmt:
	@poetry run black .
	@poetry run isort . --profile black

check:
	@poetry run black --check .
	@poetry run isort . --check-only --profile black

lint:
	@poetry run pylint tensorlake
	@poetry run black --check .

test:
	@poetry run pytest

.PHONY: build format lint test version

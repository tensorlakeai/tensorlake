all: build

build:
	@rm -rf dist
	@poetry install --with=dev
	@$(MAKE) build_cloud_sdk
	@poetry build
	@cp tensorlake.data/scripts/* $$(poetry env info --path)/bin/ 2>/dev/null || true

# Local development install: builds the Rust Cloud SDK extension and installs the
# Python SDK into the active Poetry virtualenv so helper scripts such as
# `tensorlake-deploy`, `function-executor`, and the Function Service runner are on PATH.
# Note: proto stubs are pre-generated and committed; run `make build_proto`
# separately only when you change .proto files.
install-dev:
	@poetry install --with=dev
	@poetry run maturin develop
	@cp tensorlake.data/scripts/* $$(poetry env info --path)/bin/
	@echo "Done. Activate the venv with 'poetry shell' or prefix commands with 'poetry run'."

# Same as install-dev but compiles the Rust Cloud SDK extension with optimisations.
install-dev-release:
	@poetry install --with=dev
	@poetry run maturin develop --release
	@cp tensorlake.data/scripts/* $$(poetry env info --path)/bin/
	@echo "Done. Activate the venv with 'poetry shell' or prefix commands with 'poetry run'."

# Global install: builds the Python SDK wheel with the Rust Cloud SDK extension
# and installs it into the user's Python environment (~/.local/).
# After running, ensure ~/.local/bin is on your PATH.
install-global:
	@rm -rf dist
	@poetry run maturin build --release --out dist
	@pip3 install --user --break-system-packages --force-reinstall dist/tensorlake-*.whl
	@echo "Done. Make sure ~/.local/bin is on your PATH."
	@echo "  fish: fish_add_path ~/.local/bin"
	@echo "  bash/zsh: export PATH=\"\$$HOME/.local/bin:\$$PATH\""

# Function Executor protocol sources are shared by the Python and TypeScript
# runtimes. Python bindings remain generated inside the Python package.
PROTO_SOURCE_ROOT=proto
PROTO_PACKAGE_PATH=tensorlake/function_executor/proto
PROTO_SOURCE_DIR=${PROTO_SOURCE_ROOT}/${PROTO_PACKAGE_PATH}
PROTO_GENERATED_DIR=src/${PROTO_PACKAGE_PATH}

build_proto:
	@poetry install
	@poetry run python -m grpc_tools.protoc \
		--proto_path=${PROTO_SOURCE_ROOT} \
		--python_out=src \
		--pyi_out=src \
		--grpc_python_out=src \
		${PROTO_SOURCE_DIR}/status.proto \
		${PROTO_SOURCE_DIR}/function_executor.proto
	@#The generated proto files don't pass linter checks and need to get reformatted.
	@poetry run black ${PROTO_GENERATED_DIR}
	@poetry run isort ${PROTO_GENERATED_DIR} --profile black

# Build the Rust Cloud SDK PyO3 extension and install it as tensorlake._cloud_sdk
build_cloud_sdk:
	@poetry run maturin develop

# Legacy alias
build_rust_py_client: build_cloud_sdk

fmt:
	@poetry run black .
	@poetry run isort . --profile black

check:
	@poetry run black --check .
	@poetry run isort . --check-only --profile black

test:
	cd tests && ./run_tests.sh

test_document_ai:
	cd tests && ./run_tests.sh --document-ai

test_sandbox:
	@$(MAKE) build_cloud_sdk
	cd tests/sandbox && poetry run python test_lifecycle.py -v

# Launches both real function executors and drives the shared gRPC allocation
# parity matrix through bindings generated from the shared protocol source.
test_function_executor_compatibility:
	@npm --prefix typescript run check:proto
	@npm --prefix typescript run build:sdk
	@PYTHONPATH=src poetry run python tests/function_executor_compatibility/run.py

# Replicates the PyPI publish workflow locally: builds the tensorlake wheel with
# the _cloud_sdk Rust extension, then installs into a temporary venv and verifies
# imports and package scripts — all without touching PyPI.
build_release:
	@rm -rf dist
	@echo "--- Building tensorlake wheel ---"
	@poetry run maturin build --release --out dist
	@echo "--- Verifying wheel in a clean venv ---"
	@poetry run python .github/scripts/verify_wheel.py "dist/tensorlake-*.whl" tensorlake._cloud_sdk tensorlake.cli.deploy --expect-script function-executor --expect-script tensorlake-python-function-runner --expect-script tensorlake-deploy --reject-script tl --reject-script tensorlake
	@echo "--- Done. Wheel is in dist/ ---"

bump_version:
	@test -n "$(VERSION)" || (echo "Usage: make bump_version VERSION=x.y.z" && exit 1)
	@python3 .github/scripts/bump_version.py "$(VERSION)"

.PHONY: all build build_proto build_cloud_sdk build_rust_py_client fmt check test test_document_ai test_sandbox test_function_executor_compatibility install-dev install-dev-release install-global build_release bump_version fs-posix-conformance

# POSIX conformance for `tl fs mount` (Linux + FUSE + working credentials). Runs the ported
# issue-#24 battery against a real mounted workspace, in fresh and snapshot-reattached phases.
# Build tl first (cargo build -p tl) or point TL_BIN at a binary.
fs-posix-conformance:
	bash tests/fs-posix-conformance/run_conformance.sh

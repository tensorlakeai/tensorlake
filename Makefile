all: build

build:
	@rm -rf dist
	@poetry install --with=dev
	@$(MAKE) build_cloud_sdk
	@poetry build
	@cp tensorlake.data/scripts/* $$(poetry env info --path)/bin/ 2>/dev/null || true

# Local development install: builds the Rust CLI (debug) and installs everything
# into the active Poetry virtualenv so that `tl`, `tensorlake`, and all wrapper
# scripts (tensorlake-deploy, tensorlake-parse, etc.) are on PATH.
# Note: proto stubs are pre-generated and committed; run `make build_proto`
# separately only when you change .proto files.
install-dev:
	@poetry install --with=dev
	@poetry run maturin develop
	@cp tensorlake.data/scripts/* $$(poetry env info --path)/bin/
	@echo "Done. Activate the venv with 'poetry shell' or prefix commands with 'poetry run'."

# Same as install-dev but compiles the Rust CLI with optimisations (slower build,
# faster binary — useful for manual performance testing).
install-dev-release:
	@poetry install --with=dev
	@poetry run maturin develop --release
	@cp tensorlake.data/scripts/* $$(poetry env info --path)/bin/
	@echo "Done. Activate the venv with 'poetry shell' or prefix commands with 'poetry run'."

# Global install: builds the Rust CLI (release) and installs the Python package
# into the user's Python environment (~/.local/) so that `tl`, `tensorlake`, and
# all wrapper scripts work from any directory without activating a virtualenv.
# Requires: pip and maturin available outside any virtualenv.
# After running, ensure ~/.local/bin is on your PATH.
install-global:
	@pip3 install --user -e .
	@mkdir -p ~/.local/bin
	@cp tensorlake.data/scripts/* ~/.local/bin/
	@chmod +x ~/.local/bin/tensorlake-*
	@echo "Done. Make sure ~/.local/bin is on your PATH."
	@echo "  fish: fish_add_path ~/.local/bin"
	@echo "  bash/zsh: export PATH=\"\$$HOME/.local/bin:\$$PATH\""

# .proto file and generated Python files have to be in the same directory.
# See known issue https://github.com/grpc/grpc/issues/29459.
PROTO_DIR_PATH_INSIDE_PACKAGE=tensorlake/function_executor/proto
PROTO_DIR_PATH=src/${PROTO_DIR_INSIDE_PACKAGE}

build_proto:
	@poetry install
	@cd src && poetry run python -m grpc_tools.protoc \
		--proto_path=. \
		--python_out=. \
		--pyi_out=. \
		--grpc_python_out=. \
		${PROTO_DIR_PATH_INSIDE_PACKAGE}/status.proto \
		${PROTO_DIR_PATH_INSIDE_PACKAGE}/function_executor.proto
	@#The generated proto files don't pass linter checks and need to get reformatted.
	@poetry run black ${PROTO_DIR_PATH}
	@poetry run isort ${PROTO_DIR_PATH} --profile black

# Build the Rust Cloud SDK PyO3 extension and install it as tensorlake._cloud_sdk
build_cloud_sdk:
	@poetry run maturin develop --manifest-path crates/rust-cloud-sdk-py/Cargo.toml

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

.PHONY: all build build_proto build_cloud_sdk build_rust_py_client fmt check test test_document_ai test_sandbox install-dev install-dev-release install-global

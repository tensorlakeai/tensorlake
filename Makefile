all: build

build: build_proto
	@rm -rf dist
	@poetry install --with=dev
	@poetry build
	@cp tensorlake.data/scripts/* $$(poetry env info --path)/bin/ 2>/dev/null || true

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

fmt:
	@poetry run black .
	@poetry run isort . --profile black

build_rust_py_client:
	@poetry run maturin develop --manifest-path crates/rust-cloud-sdk-py/Cargo.toml

check:
	@poetry run black --check .
	@poetry run isort . --check-only --profile black

test:
	cd tests && ./run_tests.sh

test_document_ai:
	cd tests && ./run_tests.sh --document-ai

test_sandbox:
	@poetry run maturin develop --manifest-path crates/rust-cloud-sdk-py/Cargo.toml
	cd tests/sandbox && poetry run python test_lifecycle.py -v

.PHONY: all build build_proto fmt build_rust_py_client test test_document_ai test_sandbox

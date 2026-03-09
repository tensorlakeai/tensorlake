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
	@pip3 install --user --break-system-packages -e .
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

# Replicates the PyPI publish workflow locally: builds the _cloud_sdk Rust
# extension, bundles its .so into the main wheel, then installs into a
# temporary venv and verifies imports — all without touching PyPI.
build_release:
	@rm -rf dist dist-cloud-sdk
	@echo "--- Building Cloud SDK extension ---"
	@poetry run maturin build --manifest-path crates/rust-cloud-sdk-py/Cargo.toml --release --out dist-cloud-sdk
	@echo "--- Bundling .so into src/tensorlake/ ---"
	@poetry run python -c "\
import zipfile, pathlib; \
d = pathlib.Path('src/tensorlake'); \
[p.unlink() for p in [*d.glob('_cloud_sdk*.so'), *d.glob('_cloud_sdk*.pyd')]]; \
w = next(pathlib.Path('dist-cloud-sdk').glob('tensorlake_rust_cloud_sdk-*.whl')); \
members = [x for x in zipfile.ZipFile(w).namelist() if pathlib.Path(x).name.startswith('_cloud_sdk') and (x.endswith('.so') or x.endswith('.pyd'))]; \
t = d / pathlib.Path(members[0]).name; \
t.write_bytes(zipfile.ZipFile(w).read(members[0])); \
print(f'Bundled {members[0]} -> {t}')"
	@echo "--- Building main wheel ---"
	@poetry run maturin build --release --out dist
	@echo "--- Removing bundled .so from source tree ---"
	@poetry run python -c "import pathlib; [p.unlink() for p in [*pathlib.Path('src/tensorlake').glob('_cloud_sdk*.so'), *pathlib.Path('src/tensorlake').glob('_cloud_sdk*.pyd')]]"
	@echo "--- Verifying wheel in a clean venv ---"
	@rm -rf /tmp/tensorlake-verify
	@poetry run python -m venv /tmp/tensorlake-verify
	@/tmp/tensorlake-verify/bin/pip install --quiet dist/tensorlake-*.whl
	@/tmp/tensorlake-verify/bin/python -c "import tensorlake._cloud_sdk; print('OK: tensorlake._cloud_sdk')"
	@/tmp/tensorlake-verify/bin/python -c "from tensorlake.cli.deploy import deploy_entrypoint; print('OK: tensorlake.cli.deploy')"
	@/tmp/tensorlake-verify/bin/python -c "from tensorlake.cli.build_images import main; print('OK: tensorlake.cli.build_images')"
	@rm -rf /tmp/tensorlake-verify
	@echo "--- Done. Wheel is in dist/ ---"

bump_version:
	@test -n "$(VERSION)" || (echo "Usage: make bump_version VERSION=x.y.z" && exit 1)
	@for f in pyproject.toml Cargo.toml crates/rust-cloud-sdk-py/pyproject.toml; do \
		python3 -c "import re, pathlib; p = pathlib.Path('$$f'); p.write_text(re.sub(r'^version = \"[^\"]*\"', 'version = \"$(VERSION)\"', p.read_text(), count=1, flags=re.MULTILINE))"; \
		echo "  Updated $$f"; \
	done
	@echo "Version bumped to $(VERSION)"

.PHONY: all build build_proto build_cloud_sdk build_rust_py_client fmt check test test_document_ai test_sandbox install-dev install-dev-release install-global build_release bump_version

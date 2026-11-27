all: build

build:
	@rm -rf dist
	@poetry install --with=dev
	@$(MAKE) build_cloud_sdk
	@poetry build --format wheel

# Local development install: builds the Rust Cloud SDK extension and installs the
# Python SDK into the active Poetry virtualenv.
install-dev:
	@poetry install --with=dev
	@poetry run maturin develop
	@echo "Done. Activate the venv with 'poetry shell' or prefix commands with 'poetry run'."

# Same as install-dev but compiles the Rust Cloud SDK extension with optimisations.
install-dev-release:
	@poetry install --with=dev
	@poetry run maturin develop --release
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

test_sandbox:
	@$(MAKE) build_cloud_sdk
	cd tests/sandbox && poetry run python test_lifecycle.py -v

# Replicates the PyPI publish workflow locally: builds the tensorlake wheel with
# the _cloud_sdk Rust extension, then installs into a temporary venv and verifies
# imports and package scripts — all without touching PyPI.
build_release:
	@rm -rf dist
	@echo "--- Building tensorlake wheel ---"
	@poetry run maturin build --release --out dist
	@echo "--- Verifying wheel in a clean venv ---"
	@poetry run python .github/scripts/verify_wheel.py "dist/tensorlake-*.whl" tensorlake._cloud_sdk --reject-script tl --reject-script tensorlake
	@echo "--- Done. Wheel is in dist/ ---"

bump_version:
	@test -n "$(VERSION)" || (echo "Usage: make bump_version VERSION=x.y.z" && exit 1)
	@python3 .github/scripts/bump_version.py "$(VERSION)"

.PHONY: all build build_cloud_sdk build_rust_py_client fmt check test test_sandbox install-dev install-dev-release install-global build_release bump_version fs-posix-conformance

# POSIX conformance for `tl fs mount` (Linux + FUSE + working credentials). Runs the ported
# issue-#24 battery against a real mounted workspace, in fresh and snapshot-reattached phases.
# Build tl first (cargo build -p tl) or point TL_BIN at a binary.
fs-posix-conformance:
	bash tests/fs-posix-conformance/run_conformance.sh

# Filesystem placement wire contract and both public SDK wrappers (no cloud credentials).
CARGO ?= cargo
PYTHON ?= poetry run python
.PHONY: test_filesystem_placement test_filesystem_placement_wrappers

test_filesystem_placement:
	@$(CARGO) test -p tensorlake --lib filesystem_region_
	@$(CARGO) clippy -p tensorlake -p tensorlake-rust-cloud-sdk-py -p tensorlake-rust-cloud-sdk-node --all-targets --no-deps -- -D warnings

test_filesystem_placement_wrappers:
	PYTHONPATH=src $(PYTHON) -m unittest discover -s tests/filesystem -p 'test_filesystem_unit.py'
	PYTHONPATH=src $(PYTHON) -m unittest tests.sandbox.test_file_systems
	npm --prefix typescript run typecheck
	npm --prefix typescript test

# Socket-free wire checks complement the full placement retry/transport gate above.
.PHONY: test_filesystem_placement_unit
test_filesystem_placement_unit:
	@$(CARGO) test -p tensorlake --lib artifact_storage::models::placement_tests
	@$(CARGO) clippy -p tensorlake -p tensorlake-rust-cloud-sdk-py -p tensorlake-rust-cloud-sdk-node --all-targets --no-deps -- -D warnings

# In-process wrapper checks; the full wrapper target retains the socket integration suite.
.PHONY: test_filesystem_placement_wrapper_unit
test_filesystem_placement_wrapper_unit:
	PYTHONPATH=src $(PYTHON) -m unittest discover -s tests/filesystem -p 'test_filesystem_unit.py'
	PYTHONPATH=src $(PYTHON) -m unittest tests.sandbox.test_file_systems
	npm --prefix typescript run typecheck
	npm --prefix typescript test -- --cache=false tests/filesystem.test.ts tests/sandbox.test.ts

# Function executor protocol

This directory is the single source of truth for the function-executor gRPC
protocol used by both the Python and TypeScript runtimes.

- Python bindings are generated into `src/tensorlake/function_executor/proto`
  with `make build_proto`.
- The TypeScript executor loads this source directly in a development checkout.
  `npm --prefix typescript run build:sdk` copies it into the self-contained
  executor capsule used by published packages and deployed function images.

Change the protocol here, regenerate the Python bindings, and run both function
executor test suites. Do not add language-specific copies of the `.proto`
sources.

Run `make test_function_executor_compatibility` to launch both executors and
compare their normalized behavior across the shared durable allocation matrix.
See `tests/function_executor_compatibility/README.md` for coverage and setup.

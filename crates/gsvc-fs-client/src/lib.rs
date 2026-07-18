//! Resolution-only placeholder for the private TensorLake filesystem client implementation.

compile_error!(
    "the `mount` feature requires the private gsvc-fs-client implementation. Build with `just \
     build-cli-full` or `just test-cli-full`, which vendors it from artifact_storage."
);

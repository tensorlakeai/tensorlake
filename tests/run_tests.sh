#!/bin/bash

enable_cli_test_suite=false
enable_utils_test_suite=false
enable_filesystem_test_suite=false
if [ "$1" == "--filesystem" ]; then
  enable_filesystem_test_suite=true
else
  # All by default
  enable_cli_test_suite=true
  enable_utils_test_suite=true
  enable_filesystem_test_suite=true
fi

tests_exit_code=0

run_test_suite() {
  local test_files=$1
  local test_suite_name=$2
  local test_suite_exit_code=0

  # Run each test file one by one sequentially. Set $tests_exit_code to non zero
  # value if any of the test commands return non zero status code. Don't
  # stop if a test command fails.
  for test_file in $test_files; do
    echo "Running $test_file for $test_suite_name test suite"
    poetry run python $test_file
    local test_file_exit_code=$?
    if [ $test_file_exit_code -ne 0 ]; then
      echo "One or more tests failed in $test_file for $test_suite_name test suite." | tee -a $summary_file
    fi
    tests_exit_code=$((tests_exit_code || test_file_exit_code))
  done
}

# cd to the script's directory.
cd "$(dirname "$0")"

echo "Building Rust Cloud SDK Python bindings..."
poetry run maturin develop --manifest-path ../crates/rust-cloud-sdk-py/Cargo.toml
if [ $? -ne 0 ]; then
  echo "Failed to build Rust Cloud SDK Python bindings." 1>&2
  exit 1
fi

summary_file=".run_tests_summary.txt"
rm -f $summary_file

cli_test_files=$(find ./cli -name 'test_*.py')
utils_test_files=$(find ./utils -name 'test_*.py')
filesystem_test_files=$(find ./filesystem -name 'test_*.py')

if [ "$enable_cli_test_suite" = true ]; then
  run_test_suite "$cli_test_files" "CLI"
fi

if [ "$enable_filesystem_test_suite" = true ]; then
  run_test_suite "$filesystem_test_files" "Filesystem"
fi


if [ $tests_exit_code -eq 0 ]; then
  echo "All tests passed!" >> $summary_file
else
  echo "One or more tests failed. Please check output log for details." >> $summary_file
fi

cat $summary_file
exit $tests_exit_code

import argparse
import faulthandler
import os
from typing import Any

from ..applications.internal_logger import InternalLogger
from .info import info_response_kv_args
from .server import Server
from .service import Service


def validate_args(args, logger: Any):
    if args.address is None:
        logger.error("--address argument is required")
        exit(1)

    if args.executor_id is None:
        logger.error("--executor-id argument is required")
        exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Runs Function Executor with the specified API server address"
    )
    parser.add_argument(
        "--executor-id",
        help="ID of Executor that started this Function Executor",
        type=str,
    )
    parser.add_argument(
        "--function-executor-id",
        help="ID of this Function Executor",
        type=str,
        default="",
    )
    parser.add_argument("--address", help="API server address to listen on", type=str)

    # Don't fail if unknown arguments are present. This supports backward compatibility when new args are added.
    args, ignored_args = parser.parse_known_args()

    logger = InternalLogger.get_logger(module=__name__)
    try:
        faulthandler.enable(all_threads=True)
    except Exception as e:
        logger.warning("failed to enable faulthandler", exc_info=e)
    validate_args(args, logger)

    logger = logger.bind(
        executor_id=args.executor_id,
        fn_executor_id=args.function_executor_id,
        **info_response_kv_args(),
    )
    logger.info("starting function executor server", address=args.address)
    if len(ignored_args) > 0:
        logger.warning("ignored cli arguments", ignored_args=ignored_args)

    Server(
        server_address=args.address,
        service=Service(logger),
        # A hard timeout means terminal draining or transport shutdown failed.
        # Report that distinction to the supervising executor.
        force_exit=lambda: os._exit(1),
    ).run()
    try:
        # InternalLogger flushes its captured destination for every message.
        # Do not touch the current sys.stdout/sys.stderr here: application code
        # runs in-process and may have replaced either object with a blocking
        # implementation after the server shutdown watchdog was cancelled.
        logger.info("stopped function executor server")
    finally:
        # Running BLOB operations use worker threads that may be blocked in kernel
        # I/O and cannot be cancelled by ThreadPoolExecutor. The gRPC grace period
        # above has already delivered terminal allocation events, so exit without
        # waiting for those non-daemon workers, matching Node's process.exit.
        os._exit(0)


if __name__ == "__main__":
    main()

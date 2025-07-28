import argparse
import multiprocessing as mp
from typing import Any

from .info import info_response_kv_args
from .logger import FunctionExecutorLogger
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
    # Set "spawn" method because grpc Server only works correctly if there's exec after fork.
    mp.set_start_method("spawn")
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

    logger = FunctionExecutorLogger.get_logger(module=__name__)
    validate_args(args, logger)

    logger = logger.bind(
        executor_id=args.executor_id,
        fn_executor_id=args.function_executor_id,
        **info_response_kv_args()
    )
    logger.info("starting function executor server", address=args.address)
    if len(ignored_args) > 0:
        logger.warning("ignored cli arguments", ignored_args=ignored_args)

    Server(
        server_address=args.address,
        service=Service(logger),
    ).run()


if __name__ == "__main__":
    main()

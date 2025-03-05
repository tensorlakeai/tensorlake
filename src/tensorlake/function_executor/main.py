from tensorlake.utils.logging import (
    configure_development_mode_logging,
    configure_logging_early,
    configure_production_mode_logging,
)

configure_logging_early()

import argparse
from typing import Any

import structlog

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
    parser.add_argument("--address", help="API server address to listen on", type=str)
    parser.add_argument(
        "-d", "--dev", help="Run in development mode", action="store_true"
    )
    # TODO: Add --function-executor-id after all used Function Executors allow unknown CLI arguments.

    # Don't fail if unknown arguments are present. This supports backward compatibility when new args are added.
    args, ignored_args = parser.parse_known_args()

    if args.dev:
        configure_development_mode_logging()
    else:
        configure_production_mode_logging()

    logger = structlog.get_logger(module=__name__)
    validate_args(args, logger)

    # TODO: Add function-executor-id to logger context.
    logger = logger.bind(executor_id=args.executor_id, **info_response_kv_args())
    logger.info("starting function executor server", address=args.address, dev=args.dev)
    if len(ignored_args) > 0:
        logger.warning("ignored cli arguments", ignored_args=ignored_args)

    Server(
        server_address=args.address,
        service=Service(logger),
    ).run()


if __name__ == "__main__":
    main()

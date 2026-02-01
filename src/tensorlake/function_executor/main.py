import argparse
import sys
from typing import Any

from ..applications.internal_logger import InternalLogger
from .info import info_response_kv_args
from .server import Server
from .service import Service


def validate_args(args, logger: Any) -> bool:
    """Validate CLI arguments.

    Returns True if validation passed, False otherwise.
    """
    # Check for auto-init mode (all auto-init args must be provided together)
    auto_init_args = [
        args.code_path,
        args.namespace,
        args.app_name,
        args.app_version,
        args.function_name,
    ]
    auto_init_mode = any(arg is not None for arg in auto_init_args)

    if auto_init_mode:
        # In auto-init mode, all auto-init args are required
        if not all(arg is not None for arg in auto_init_args):
            logger.error(
                "When using auto-initialization, all of --code-path, --namespace, "
                "--app-name, --app-version, and --function-name are required"
            )
            return False
        # In auto-init mode, --address is optional (can be HTTP-only)
        if args.address is None and args.http_port is None:
            logger.error(
                "At least one of --address (gRPC) or --http-port (HTTP) is required"
            )
            return False
    else:
        # In traditional mode, --address and --executor-id are required
        if args.address is None:
            logger.error("--address argument is required")
            return False
        if args.executor_id is None:
            logger.error("--executor-id argument is required")
            return False

    return True


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
    parser.add_argument("--address", help="gRPC server address to listen on", type=str)
    parser.add_argument(
        "--http-port",
        help="HTTP server port for allocation API (optional, enables HTTP API if set)",
        type=int,
        default=None,
    )

    # Auto-initialization arguments (for container entrypoint mode)
    parser.add_argument(
        "--code-path",
        help="Path to function code directory (enables auto-initialization)",
        type=str,
        default=None,
    )
    parser.add_argument(
        "--namespace",
        help="Function namespace (required with --code-path)",
        type=str,
        default=None,
    )
    parser.add_argument(
        "--app-name",
        help="Application name (required with --code-path)",
        type=str,
        default=None,
    )
    parser.add_argument(
        "--app-version",
        help="Application version (required with --code-path)",
        type=str,
        default=None,
    )
    parser.add_argument(
        "--function-name",
        help="Function name (required with --code-path)",
        type=str,
        default=None,
    )

    # Don't fail if unknown arguments are present. This supports backward compatibility when new args are added.
    args, ignored_args = parser.parse_known_args()

    logger = InternalLogger.get_logger(module=__name__)
    if not validate_args(args, logger):
        sys.exit(1)

    logger = logger.bind(
        executor_id=args.executor_id or "auto-init",
        fn_executor_id=args.function_executor_id,
        **info_response_kv_args()
    )
    logger.info(
        "starting function executor server",
        address=args.address,
        http_port=args.http_port,
        code_path=args.code_path,
    )
    if len(ignored_args) > 0:
        logger.warning("ignored cli arguments", ignored_args=ignored_args)

    service = Service(logger)

    # Auto-initialize if code-path is provided
    if args.code_path is not None:
        success = service.initialize_from_code_path(
            code_path=args.code_path,
            namespace=args.namespace,
            app_name=args.app_name,
            app_version=args.app_version,
            function_name=args.function_name,
        )
        if not success:
            logger.error("Failed to auto-initialize function executor")
            sys.exit(1)

    Server(
        server_address=args.address,
        service=service,
        http_port=args.http_port,
        logger=logger,
    ).run()


if __name__ == "__main__":
    main()

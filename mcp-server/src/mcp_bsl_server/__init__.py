import argparse
import logging
import os
import sys

import dotenv

# Add the repo root to sys.path so we can import constants/semantics
repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from . import server


def main():
    """Main entry point for the BSL MCP Server."""
    parser = argparse.ArgumentParser(description="BSL MCP Server")
    parser.add_argument(
        "--log_dir", required=False, default=None, help="Directory to log to"
    )
    parser.add_argument(
        "--log_level", required=False, default="INFO", help="Logging level"
    )
    parser.add_argument(
        "--pipeline_name",
        required=False,
        default=None,
        help="dlt pipeline name to attach to",
    )

    dotenv.load_dotenv()

    args, _ = parser.parse_known_args()

    pipeline_name = args.pipeline_name or os.getenv("PIPELINE_NAME", "contoso")

    logger = logging.getLogger(__name__)
    logger.propagate = False
    logger.setLevel(args.log_level)
    console_handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    if args.log_dir:
        file_handler = logging.FileHandler(
            os.path.join(args.log_dir, "mcp_bsl_server.log")
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    server.main(pipeline_name=pipeline_name, logger=logger)


__all__ = ["main", "server"]

if __name__ == "__main__":
    main()

"""
Materialize MCP Server

A server that provides static tools for managing and querying Materialize databases
over the Model Context Protocol (MCP). The server exposes tools for listing objects,
managing clusters, executing SQL transactions, and monitoring query performance.
"""

import asyncio
import logging
import sys
from typing import Optional

from materialize_mcp_server.mcp_server import MaterializeDevMcpServer
from .config import load_config, ConfigurationError
from .logging_config import setup_structured_logging

logger: Optional[logging.Logger] = None

async def run():
    """Main server run function."""
    global logger

    try:
        # Load and validate configuration
        cfg = load_config()
    except ConfigurationError as e:
        print(f"Configuration error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error loading configuration: {e}", file=sys.stderr)
        sys.exit(1)

    # Setup structured logging
    logger = setup_structured_logging(
        service_name=cfg.service_name,
        version=cfg.version,
        log_level=cfg.log_level,
        enable_json=cfg.enable_json_logging,
    )

    logger.info(
        "Starting Materialize MCP Server",
        extra={
            "version": cfg.version,
            "transport": cfg.transport,
            "log_level": cfg.log_level,
            "json_logging": cfg.enable_json_logging,
        },
    )

    async with MaterializeDevMcpServer(cfg) as server:
        await server.run()


def main():
    """Synchronous wrapper for the async main function."""
    global logger

    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        if logger:
            logger.info("Received shutdown signal, stopping server gracefully")
        else:
            print("Received shutdown signal, stopping server", file=sys.stderr)
    except ConfigurationError as e:
        print(f"Configuration error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        if logger:
            logger.error(
                "Fatal error in main",
                extra={"error_type": type(e).__name__, "error_message": str(e)},
                exc_info=True,
            )
        else:
            print(f"Fatal error: {e}", file=sys.stderr)
            import traceback

            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

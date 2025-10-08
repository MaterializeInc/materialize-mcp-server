from mcp.server import FastMCP
from psycopg_pool import AsyncConnectionPool
import asyncio
import logging
from enum import Enum

from .config import Config
from .system_prompt import INSTRUCTIONS
from .tools.check_connection import CheckConnection
from .tools.execute_ddl import ExecuteDDL
from .tools.monitor_data_freshness import MonitorDataFreshness
from .tools.object_freshness_diagnostics import (
    ObjectFreshnessDiagnostics,
)
from .tools.run_sql_transaction import RunSqlTransaction
from .tools.search_documentation import SearchDocumentation

logger = logging.getLogger(__name__)


class ConnectionState(Enum):
    """Connection state for Materialize database."""

    UNKNOWN = "unknown"
    CONNECTED = "connected"
    FAILED = "failed"


class LazyAsyncConnectionPool:
    """Wrapper that lazily initializes AsyncConnectionPool on first use."""

    def __init__(self, dsn: str, min_size: int, max_size: int):
        self._dsn = dsn
        self._min_size = min_size
        self._max_size = max_size
        self._pool = None
        self._lock = asyncio.Lock()
        self._state = ConnectionState.UNKNOWN
        self._last_error = None

    @property
    def state(self) -> ConnectionState:
        """Get the current connection state."""
        return self._state

    @property
    def last_error(self) -> str | None:
        """Get the last connection error message."""
        return self._last_error

    async def _ensure_initialized(self):
        """Initialize the pool if not already initialized."""
        if self._pool is None:
            async with self._lock:
                if self._pool is None:  # Double-check after acquiring lock
                    try:

                        async def configure(conn):
                            await conn.set_autocommit(True)

                        self._pool = AsyncConnectionPool(
                            conninfo=self._dsn,
                            min_size=self._min_size,
                            max_size=self._max_size,
                            kwargs={"application_name": "materialize_mcp_dev_server"},
                            configure=configure,
                        )
                        await self._pool.__aenter__()
                        self._state = ConnectionState.CONNECTED
                        self._last_error = None
                        logger.info("Successfully connected to Materialize")
                    except Exception as e:
                        self._state = ConnectionState.FAILED
                        self._last_error = str(e)
                        logger.warning(f"Failed to connect to Materialize: {e}")
                        raise

    async def check_health(self) -> tuple[bool, str]:
        """
        Check if the connection to Materialize is healthy.

        Returns:
            Tuple of (is_healthy, message)
        """
        try:
            await self._ensure_initialized()
            async with await self._pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT 1")
                    await cur.fetchone()
            return True, "Connected to Materialize"
        except Exception as e:
            return False, f"Failed to connect to Materialize: {str(e)}"

    async def connection(self):
        """Get a connection from the pool."""
        await self._ensure_initialized()
        return await self._pool.connection()

    async def close(self):
        """Close the pool if it was initialized."""
        if self._pool is not None:
            await self._pool.__aexit__(None, None, None)


class MaterializeDevMcpServer:
    def __init__(self, cfg: Config):
        self._cfg = cfg

    async def __aenter__(self) -> "MaterializeDevMcpServer":
        self._server = FastMCP(
            "materialize_dev_mcp_server",
            port=self._cfg.port,
            host=self._cfg.host,
            instructions=INSTRUCTIONS,
        )

        # Create lazy pool for non-DOCS_ONLY modes
        if self._cfg.mode != "DOCS_ONLY":
            self._pool = LazyAsyncConnectionPool(
                self._cfg.dsn,
                self._cfg.pool_min_size,
                self._cfg.pool_max_size,
            )
        else:
            self._pool = None

        if self._cfg.mode == "READ_WRITE":
            self._server.add_tool(ExecuteDDL(self._pool))

        if self._cfg.mode in ["READ_ONLY", "READ_WRITE"]:
            self._server.add_tool(RunSqlTransaction(self._pool))
            self._server.add_tool(MonitorDataFreshness(self._pool))
            self._server.add_tool(ObjectFreshnessDiagnostics(self._pool))

        # Always add connection check and documentation search tools
        self._server.add_tool(CheckConnection(self._pool))
        self._server.add_tool(SearchDocumentation())

        # Do a non-blocking health check in the background
        if self._pool is not None:
            asyncio.create_task(self._background_health_check())

        return self

    async def _background_health_check(self):
        """Perform a background health check and log the result."""
        try:
            is_healthy, message = await self._pool.check_health()
            if not is_healthy:
                logger.warning(
                    f"Materialize connection check failed: {message}. "
                    "Database tools will not be available until connection is restored. "
                    "Documentation search tools will continue to work."
                )
        except Exception as e:
            logger.warning(
                f"Background health check failed: {e}. "
                "This is not critical - tools will attempt to connect when called."
            )

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._pool is not None:
            await self._pool.close()

    async def run(self):
        match self._cfg.transport:
            case "stdio":
                await self._server.run_stdio_async()
            case "http":
                await self._server.run_streamable_http_async()

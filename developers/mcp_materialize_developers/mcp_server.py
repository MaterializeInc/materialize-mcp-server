from mcp.server import FastMCP
from psycopg_pool import AsyncConnectionPool

from materialize_mcp_server.config import Config
from materialize_mcp_server.system_prompt import INSTRUCTIONS
from materialize_mcp_server.tools.execute_ddl import ExecuteDDL
from materialize_mcp_server.tools.monitor_data_freshness import MonitorDataFreshness
from materialize_mcp_server.tools.object_freshness_diagnostics import (
    ObjectFreshnessDiagnostics,
)
from materialize_mcp_server.tools.run_sql_transaction import RunSqlTransaction
from materialize_mcp_server.tools.search_documentation import SearchDocumentation


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

        async def configure(conn):
            await conn.set_autocommit(True)

        self._pool = AsyncConnectionPool(
            conninfo=self._cfg.dsn,
            min_size=self._cfg.pool_min_size,
            max_size=self._cfg.pool_max_size,
            kwargs={"application_name": "materialize_mcp_dev_server"},
            configure=configure,
        )
        await self._pool.__aenter__()

        self._server.add_tool(ExecuteDDL(self._pool))
        self._server.add_tool(RunSqlTransaction(self._pool))
        self._server.add_tool(MonitorDataFreshness(self._pool))
        self._server.add_tool(ObjectFreshnessDiagnostics(self._pool))
        self._server.add_tool(SearchDocumentation())
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._pool.__aexit__(exc_type, exc_val, exc_tb)

    async def run(self):
        match self._cfg.transport:
            case "stdio":
                await self._server.run_stdio_async()
            case "http":
                await self._server.run_streamable_http_async()

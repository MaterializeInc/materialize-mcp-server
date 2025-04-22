"""
Materialize MCP Server

A  server that exposes Materialize indexes as "tools" over the Model Context
Protocol (MCP).  Each Materialize index that the connected role is allowed to
`SELECT` from (and whose cluster it can `USAGE`) is surfaced as a tool whose
inputs correspond to the indexed columns and whose output is the remaining
columns of the underlying view.

The server supports two transports:

* stdio – lines of JSON over stdin/stdout (handy for local CLIs)
* sse   – server‑sent events suitable for web browsers

---------------

1.  ``list_tools`` executes a catalog query to derive the list of exposable
    indexes; the result is translated into MCP ``Tool`` objects.
2.  ``call_tool`` validates the requested tool, switches the session to the
    appropriate cluster, executes a parameterised ``SELECT`` against the
    indexed view, and returns the first matching row (minus any columns whose
    values were supplied as inputs).
"""

import argparse
import asyncio
from dataclasses import dataclass
import logging
import os
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Tuple, Optional, Sequence, AsyncIterator

import simplejson as json
from mcp.server import FastMCP
from mcp.types import Tool, TextContent, ImageContent, EmbeddedResource
from psycopg import sql
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool


logger = logging.getLogger("mz_mcp_server")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


@dataclass(frozen=True)
class Settings:
    dsn: str
    transport: str


def load_settings() -> Settings:
    parser = argparse.ArgumentParser(description="Run Materialize MCP server")
    parser.add_argument(
        "--transport",
        choices=["stdio", "sse"],
        default=os.getenv("MCP_TRANSPORT", "stdio"),
        help="Communication transport (default: stdio)",
    )

    parser.add_argument(
        "--mz-dsn",
        default=os.getenv(
            "MZ_DSN", "postgresql://materialize@localhost:6875/materialize"
        ),
        help="Materialize DSN (default: postgresql://materialize@localhost:6875/materialize)",
    )
    args = parser.parse_args()
    return Settings(dsn=args.mz_dsn, transport=args.transport)


def get_tool_query(tool: Optional[str] = None) -> Tuple[sql.Composed | sql.SQL, Tuple]:
    """
    Construct the SQL query that enumerates eligible indexes.

    When *tool* is supplied, the query is narrowed to the given *index_name*.
    The function returns a pair ``(query, params)`` ready to be passed to
    :py:meth:`cursor.execute`.

    The query inspects Materialize's system catalog to:

    * find every object the connected role can `SELECT`
    * ensure the role can `USAGE` the cluster that object lives in
    * bundle the indexed columns into a JSON schema representing tool inputs
    """
    base = sql.SQL(
        """
        WITH tools AS (
            SELECT
                op.database,
                op.schema,
                op.name AS object_name,
                i.name AS index_name,
                c.name AS cluster,
                cts.comment AS description,
                jsonb_build_object(
                    'type', 'object',
                    'required', jsonb_agg(ccol.name),
                    'properties', jsonb_object_agg(
                        ccol.name,
                        jsonb_build_object(
                            'type',
                            CASE
                                WHEN ccol.name IN ('int', 'bigint', 'float', 'numeric') THEN 'number'
                                WHEN ccol.name = 'bool' THEN 'boolean'
                                ELSE 'text'
                            END
                        )
                    )
                ) AS input_schema
            FROM mz_internal.mz_show_my_object_privileges op
            JOIN mz_objects o ON op.name = o.name AND op.object_type = o.type
            JOIN mz_schemas s ON s.name = op.schema AND s.id = o.schema_id
            JOIN mz_databases d ON d.name = op.database AND d.id = s.database_id
            JOIN mz_indexes i ON i.on_id = o.id
            JOIN mz_index_columns ic ON i.id = ic.index_id
            JOIN mz_columns ccol ON ccol.id = o.id AND ccol.position = ic.on_position
            JOIN mz_clusters c ON c.id = i.cluster_id
            JOIN mz_internal.mz_show_my_cluster_privileges cp ON cp.name = c.name
            LEFT JOIN mz_internal.mz_comments cts ON cts.id = o.id AND cts.object_sub_id IS NULL
            WHERE op.privilege_type = 'SELECT'
              AND cp.privilege_type = 'USAGE'
            GROUP BY 1,2,3,4,5,6
        )
        SELECT * FROM tools
        WHERE 1 = 1
        """
    )
    if tool is not None:
        base += sql.SQL(" AND index_name = %s")
        return base, (tool,)
    return base, ()


@dataclass
class AppContext:
    pool: AsyncConnectionPool


def get_lifespan(settings):
    @asynccontextmanager
    async def lifespan(server) -> AsyncIterator[AppContext]:
        logger.info(
            "Initializing database connection pool for dsn {}".format(settings.dsn)
        )

        async with AsyncConnectionPool(
            conninfo=settings.dsn, min_size=1, max_size=10
        ) as pool:
            try:
                yield AppContext(pool=pool)
            finally:
                await pool.close()

    return lifespan


# —————————————————————————————————————————————————————————
# Subclass FastMCP
# —————————————————————————————————————————————————————————
class MaterializeMCP(FastMCP):
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        super().__init__(
            name="Materialize MCP Server",
            lifespan=get_lifespan(settings),
            host="0.0.0.0",
            port=3001,
            log_level="INFO",
        )

    async def list_tools(self) -> List[Tool]:
        """
        Return the catalog of available tools.

        A tool is essentially an indexed view that the current role can query.
        The metadata is discovered via :func:`get_tool_query`.

        TODO: The server could subscribe to the database catalog
        TODO: and notify the client whenever a new tool is created.
        """
        logger.info("Listing tools via SQL")
        pool = self.get_context().request_context.lifespan_context.pool
        tools: List[Tool] = []
        async with pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                query, params = get_tool_query()
                await cur.execute(query, params)
                async for row in cur:
                    desc = row.get("description")
                    if not desc:
                        logger.warning(f"Tool {row['index_name']} missing description")
                        continue
                    tools.append(
                        Tool(
                            name=row["index_name"],
                            description=desc,
                            inputSchema=row["input_schema"],
                        )
                    )
        return tools

    async def call_tool(
        self, name: str, arguments: Dict[str, Any]
    ) -> Sequence[TextContent | ImageContent | EmbeddedResource]:
        logger.info(f"Calling tool {name} with args {arguments}")
        pool = self.get_context().request_context.lifespan_context.pool
        async with pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                q_meta, p_meta = get_tool_query(name)
                await cur.execute(q_meta, p_meta)
                meta = await cur.fetchone()

        if not meta:
            raise RuntimeError(f"Tool not found: {name}")

        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    sql.SQL("SET cluster TO {};").format(
                        sql.Identifier(meta["cluster"])
                    )
                )
                await cur.execute(
                    sql.SQL("SELECT * FROM {} WHERE {};").format(
                        sql.Identifier(
                            meta["database"], meta["schema"], meta["object_name"]
                        ),
                        sql.SQL(" AND ").join(
                            [
                                sql.SQL("{} = {}").format(
                                    sql.Identifier(k), sql.Placeholder()
                                )
                                for k in arguments.keys()
                            ]
                        ),
                    ),
                    list(arguments.values()),
                )
                row = await cur.fetchone()
                if not row:
                    return []

                columns = [desc.name for desc in cur.description]
                result = {
                    k: v
                    for k, v in dict(zip(columns, row)).items()
                    if k not in arguments
                }
                return [TextContent(text=json.dumps(result), type="text")]


async def main():
    settings = load_settings()
    server = MaterializeMCP(settings)

    if settings.transport == "stdio":
        await server.run_stdio_async()
    elif settings.transport == "sse":
        await server.run_sse_async()
    else:
        raise ValueError(f"Unknown transport: {settings.transport}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down …")

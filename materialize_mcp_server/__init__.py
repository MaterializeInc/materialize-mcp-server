"""
Materialize MCP Server

A server that provides static tools for managing and querying Materialize databases
over the Model Context Protocol (MCP). The server exposes tools for listing objects,
managing clusters, executing SQL transactions, and monitoring query performance.

The server supports two transports:

* stdio – lines of JSON over stdin/stdout (handy for local CLIs)
* sse   – server‑sent events suitable for web browsers

Available Tools:

1.  ``list_objects`` - Lists all queryable objects in the database
2.  ``list_clusters`` - Lists all clusters in the Materialize instance
3.  ``create_cluster`` - Creates a new cluster with specified name and size
4.  ``run_sql_transaction`` - Executes SQL statements within a transaction
6.  ``list_schemas`` - Lists schemas, optionally filtered by database name
7.  ``list_indexes`` - Lists indexes, optionally filtered by schema and/or cluster
8.  ``create_index`` - Creates a default index on a source, view, or materialized view
9.  ``drop_index`` - Drops an index with optional CASCADE support
10. ``create_view`` - Creates a view with the specified name and SQL query
11. ``show_sources`` - Shows sources, optionally filtered by schema and/or cluster
12. ``create_postgres_connection`` - Creates a PostgreSQL connection in Materialize
13. ``create_postgres_source`` - Creates a PostgreSQL source using an existing connection and publication
14. ``create_materialized_view`` - Creates a materialized view with the specified name, cluster, and SQL query
15. ``list_materialized_views`` - Lists materialized views in Materialize, optionally filtered by schema and/or cluster
16. ``monitor_data_freshness`` - Monitors data freshness with dependency-aware analysis, showing lagging objects and their dependency chains to identify root causes
17. ``get_object_freshness_diagnostics`` - Get detailed freshness diagnostics for a specific object, showing its freshness and complete dependency chain
18. ``search_documentation`` - Search Materialize documentation using semantic search
"""

import asyncio
import json
import sys
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Sequence, AsyncIterator

import uvicorn
from mcp import stdio_server
from mcp.server.sse import SseServerTransport
from psycopg.rows import dict_row

from .mz_client import MzClient, MissingTool, json_serial
from .config import load_config, ConfigurationError
from .logging_config import setup_structured_logging, set_correlation_id, generate_correlation_id, log_tool_call, log_tool_result
from .observability import get_metrics_collector, HealthChecker, track_tool_call
from .validation import ValidationError
from materialize_mcp_server.search.doc_search import get_searcher
from mcp.server import Server, NotificationOptions
from mcp.types import Tool, TextContent, ImageContent, EmbeddedResource
from psycopg_pool import AsyncConnectionPool
from .system_prompt import INSTRUCTIONS

# Global logger - will be configured properly in main()
logger = None


def get_lifespan(cfg, metrics_collector, health_checker):
    @asynccontextmanager
    async def lifespan(server) -> AsyncIterator[MzClient]:
        logger.info(
            "Initializing connection pool",
            extra={
                "pool_min_size": cfg.pool_min_size,
                "pool_max_size": cfg.pool_max_size,
                "dsn_host": cfg.dsn.split('@')[1].split('/')[0] if '@' in cfg.dsn else "unknown"
            }
        )

        async def configure(conn):
            await conn.set_autocommit(True)
            logger.debug("Configured new database connection")

        try:
            logger.debug("Creating connection pool...")
            print(f"DEBUG: About to create connection pool", file=sys.stderr)
            async with AsyncConnectionPool(
                conninfo=cfg.dsn,
                min_size=cfg.pool_min_size,
                max_size=cfg.pool_max_size,
                kwargs={"application_name": "materialize_mcp_server"},
                configure=configure,
            ) as pool:
                try:
                    logger.debug("Testing database connection...")
                    async with pool.connection() as conn:
                        await conn.set_autocommit(True)
                        async with conn.cursor(row_factory=dict_row) as cur:
                            await cur.execute(
                                "SELECT mz_environment_id() AS env, current_role AS role;"
                            )
                            meta = await cur.fetchone()
                            logger.info(
                                f"Successfully connected to Materialize environment {meta['env']} as user {meta['role']}"
                            )
                    logger.debug("Connection pool initialized successfully")
                    yield MzClient(pool=pool)
                except Exception as e:
                    logger.error(f"Failed to initialize connection pool: {str(e)}")
                    import traceback
                    logger.error(f"Connection error traceback: {traceback.format_exc()}")
                    raise
                finally:
                    logger.info("Closing connection pool...")
                    await pool.close()
        except Exception as e:
            logger.error(f"Failed to create connection pool: {str(e)}")
            import traceback
            logger.error(f"Pool creation error traceback: {traceback.format_exc()}")
            raise

    return lifespan


async def run():
    global logger
    
    try:
        # Load and validate configuration
        cfg = load_config()
    except (ValidationError, ConfigurationError) as e:
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
        enable_json=cfg.enable_json_logging
    )
    
    # Initialize observability
    metrics_collector = get_metrics_collector()
    health_checker = HealthChecker(None, logger)  # pool will be set in lifespan
    
    logger.info(
        "Starting Materialize MCP Server",
        extra={
            "version": cfg.version,
            "transport": cfg.transport,
            "log_level": cfg.log_level,
            "json_logging": cfg.enable_json_logging
        }
    )
    
    server = Server(
        cfg.service_name, 
        lifespan=get_lifespan(cfg, metrics_collector, health_checker),
        instructions=INSTRUCTIONS)

    @server.list_tools()
    async def list_tools() -> List[Tool]:
        logger.debug("Listing available tools...")
        # Only expose static tools
        tools = []
        run_sql_transaction_tool = Tool(
            name="run_sql_transaction",
            description="Execute one or more SQL statements within a single transaction on a specified cluster. IMPORTANT: Always search documentation first before using this tool to verify correct Materialize SQL syntax and best practices.",
            inputSchema={
                "type": "object",
                "properties": {
                    "cluster_name": {
                        "type": "string",
                        "description": "Name of the cluster to execute the transaction on"
                    },
                    "sql_statements": {
                        "type": "array",
                        "items": {
                            "type": "string"
                        },
                        "description": "List of SQL statements to execute within the transaction"
                    },
                    "isolation_level": {
                        "type": "string",
                        "description": "Optional isolation level to set (e.g., 'strict serializable', 'serializable', etc.)"
                    }
                },
                "required": ["cluster_name", "sql_statements"]
            }
        )
        tools.append(run_sql_transaction_tool)

        # Add the monitor_data_freshness tool
        monitor_data_freshness_tool = Tool(
            name="monitor_data_freshness",
            description="Monitor data freshness with dependency-aware analysis. Shows lagging objects, their dependency chains, and critical paths that introduce delay to help identify root causes of freshness issues.",
            inputSchema={
                "type": "object",
                "properties": {
                    "threshold_seconds": {
                        "type": "number",
                        "description": "Freshness threshold in seconds (supports fractional values, default: 3.0)"
                    },
                    "schema": {
                        "type": "string",
                        "description": "Optional schema name to filter objects"
                    },
                    "cluster": {
                        "type": "string",
                        "description": "Optional cluster name to filter objects"
                    }
                },
                "required": []
            }
        )
        tools.append(monitor_data_freshness_tool)
        # Add the get_object_freshness_diagnostics tool
        get_object_freshness_diagnostics_tool = Tool(
            name="get_object_freshness_diagnostics",
            description="Get detailed freshness diagnostics for a specific object, showing its freshness and the complete dependency chain with freshness information for each dependency.",
            inputSchema={
                "type": "object",
                "properties": {
                    "object_name": {
                        "type": "string",
                        "description": "Name of the object to analyze"
                    },
                    "schema": {
                        "type": "string",
                        "description": "Optional schema name (default: 'public')"
                    }
                },
                "required": ["object_name"]
            }
        )
        tools.append(get_object_freshness_diagnostics_tool)
        # Add the search_documentation tool
        search_documentation_tool = Tool(
            name="search_documentation",
            description="Search Materialize documentation using semantic search to find relevant information and answers. Always search the documentation before writing SQL as details of how Materialize works may have changed.",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The search query or question to find relevant documentation"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of results to return (default: 5)"
                    },
                    "section_filter": {
                        "type": "string",
                        "description": "Optional section filter to limit search to specific documentation sections"
                    }
                },
                "required": ["query"]
            }
        )
        tools.append(search_documentation_tool)
        return tools

    @server.call_tool()
    async def call_tool(
        name: str, arguments: Dict[str, Any]
    ) -> Sequence[TextContent | ImageContent | EmbeddedResource]:
        logger.debug(f"Calling tool '{name}' with arguments: {arguments}")
        if name == "run_sql_transaction":
            try:
                cluster_name = arguments.get("cluster_name")
                sql_statements = arguments.get("sql_statements")
                isolation_level = arguments.get("isolation_level")
                if not cluster_name or not sql_statements:
                    raise ValueError("Both cluster_name and sql_statements are required")
                if not isinstance(sql_statements, list):
                    raise ValueError("sql_statements must be a list of strings")
                result = await server.request_context.lifespan_context.run_sql_transaction(
                    cluster_name, sql_statements, isolation_level
                )
                result_text = json.dumps(result, default=json_serial, indent=2)
                logger.debug(f"run_sql_transaction executed successfully: {result['message']}")
                return [TextContent(text=result_text, type="text")]
            except Exception as e:
                logger.error(f"Error executing run_sql_transaction: {str(e)}")
                raise
        if name == "monitor_data_freshness":
            try:
                threshold_seconds = arguments.get("threshold_seconds", 3.0)
                schema = arguments.get("schema")
                cluster = arguments.get("cluster")
                result = await server.request_context.lifespan_context.monitor_data_freshness(threshold_seconds, schema, cluster)
                result_text = json.dumps(result, default=json_serial, indent=2)
                logger.debug(f"monitor_data_freshness executed successfully, found {len(result)} objects")
                return [TextContent(text=result_text, type="text")]
            except Exception as e:
                logger.error(f"Error executing monitor_data_freshness: {str(e)}")
                raise
        if name == "get_object_freshness_diagnostics":
            try:
                object_name = arguments.get("object_name")
                schema = arguments.get("schema", "public")
                if not object_name:
                    raise ValueError("object_name is required")
                result = await server.request_context.lifespan_context.get_object_freshness_diagnostics(object_name, schema)
                result_text = json.dumps(result, default=json_serial, indent=2)
                logger.debug(f"get_object_freshness_diagnostics executed successfully for {object_name}")
                return [TextContent(text=result_text, type="text")]
            except Exception as e:
                logger.error(f"Error executing get_object_freshness_diagnostics: {str(e)}")
                raise
        if name == "search_documentation":
            try:
                query = arguments.get("query")
                limit = arguments.get("limit", 5)
                section_filter = arguments.get("section_filter")
                
                if not query:
                    raise ValueError("query is required")
                
                import os
                bucket_name = os.getenv("S3_BUCKET_NAME", "materialize-docs-vectors")
                searcher = await get_searcher(bucket_name)
                results = await searcher.search(query, limit, section_filter)
                
                result_text = json.dumps(results, default=json_serial, indent=2)
                logger.debug(f"search_documentation executed successfully, found {len(results)} results")
                return [TextContent(text=result_text, type="text")]
            except Exception as e:
                logger.error(f"Error executing search_documentation: {str(e)}")
                raise
        # If not a static tool, raise error
        logger.error(f"Tool not found: {name}")
        raise MissingTool(f"Tool not found: {name}")

    options = server.create_initialization_options(
        notification_options=NotificationOptions(tools_changed=True)
    )
    if cfg.transport == "stdio":
        logger.info("Starting server in stdio mode...")
        logger.info(f"Server initialization options: {options}")
        async with stdio_server() as (read_stream, write_stream):
            logger.info("stdio transport established, starting server...")
            try:
                await server.run(
                    read_stream,
                    write_stream,
                    options,
                )
            except Exception as e:
                logger.error(f"Error during server.run: {str(e)}")
                import traceback
                logger.error(f"Server run error traceback: {traceback.format_exc()}")
                raise
    elif cfg.transport == "sse":
        logger.info(f"Starting SSE server on {cfg.host}:{cfg.port}...")
        from starlette.applications import Starlette
        from starlette.routing import Mount, Route

        sse = SseServerTransport("/messages/")

        async def handle_sse(request):
            logger.debug(
                f"New SSE connection from {request.client.host if request.client else 'unknown'}"
            )
            try:
                async with sse.connect_sse(
                    request.scope, request.receive, request._send
                ) as streams:
                    await server.run(
                        streams[0],
                        streams[1],
                        options,
                    )
            except Exception as e:
                logger.error(f"Error handling SSE connection: {str(e)}")
                raise

        starlette_app = Starlette(
            routes=[
                Route("/sse", endpoint=handle_sse),
                Mount("/messages/", app=sse.handle_post_message),
            ],
        )

        config = uvicorn.Config(
            starlette_app,
            host=cfg.host,
            port=cfg.port,
            log_level=cfg.log_level.upper(),
        )
        server = uvicorn.Server(config)
        await server.serve()
    else:
        raise ValueError(f"Unknown transport: {cfg.transport}")


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
    except (ValidationError, ConfigurationError) as e:
        print(f"Configuration error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        if logger:
            logger.error(
                "Fatal error in main",
                extra={
                    "error_type": type(e).__name__,
                    "error_message": str(e)
                },
                exc_info=True
            )
        else:
            print(f"Fatal error: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

from typing import Optional

from psycopg import sql
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool


class ExecuteDDL:
    __name__ = "execute_ddl"

    def __init__(self, pool: AsyncConnectionPool):
        self._pool = pool

    async def __call__(self, ddl: list[str], cluster_name: Optional[str] = None):
        """
        Execute one or more DDL statements in Materialize.
        IMPORTANT: Always search documentation first before using this tool to verify correct Materialize SQL syntax and best practices.

        Args:
            ddl: List of ddl statements to execute
            cluster_name: Name of the cluster to execute the ddl on
        """
        async with self._pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                if cluster_name:
                    await cur.execute(
                        sql.SQL("SET cluster = {}").format(sql.Identifier(cluster_name))
                    )

                for stmt in ddl:
                    await cur.execute(stmt)

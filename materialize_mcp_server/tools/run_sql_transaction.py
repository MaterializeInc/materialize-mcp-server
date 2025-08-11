from typing import Any

from psycopg import sql
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool


class RunSqlTransaction:
    __name__ = "run_sql_transaction"

    def __init__(self, pool: AsyncConnectionPool):
        self._pool = pool

    async def __call__(
        self, cluster_name: str, sql_statements: list[str], isolation_level: str = None
    ) -> dict[str, Any]:
        """
        Execute one or more SQL SELECT statements within a single transaction.

        Args:
            cluster_name: Name of the cluster to execute the transaction on
            sql_statements: List of SQL statements to execute
            isolation_level: Optional isolation level to set (e.g., 'strict serializable', 'serializable', etc.)

        Returns:
            Dictionary with the results of the transaction execution
        """

        pool = self._pool
        results = []

        async with pool.connection() as conn:
            try:
                async with conn.cursor(row_factory=dict_row) as cur:
                    await cur.execute(
                        sql.SQL("SET cluster = {}").format(sql.Identifier(cluster_name))
                    )

                    if isolation_level:
                        await cur.execute(
                            sql.SQL("SET transaction_isolation = {}").format(
                                sql.Literal(isolation_level)
                            )
                        )

                    await cur.execute("BEGIN READ ONLY")
                    for i, sql_stmt in enumerate(sql_statements):
                        if not sql_stmt.strip():
                            continue

                        await cur.execute(sql_stmt)
                        rows = await cur.fetchall() or []
                        columns = (
                            [desc.name for desc in cur.description]
                            if cur.description
                            else []
                        )

                        results.append(
                            {
                                "statement_index": i,
                                "sql": sql_stmt,
                                "row_count": len(rows),
                                "columns": columns,
                                "rows": rows,
                            }
                        )

                    await conn.rollback()

                    return {
                        "status": "success",
                        "message": f"Transaction executed successfully on cluster '{cluster_name}'",
                        "cluster_name": cluster_name,
                        "isolation_level": isolation_level,
                        "statements_executed": len(sql_statements),
                        "results": results,
                    }

            except Exception as e:
                await conn.rollback()
                raise e

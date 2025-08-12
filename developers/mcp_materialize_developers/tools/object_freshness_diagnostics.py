from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool


class ObjectFreshnessDiagnostics:
    __name__ = "object_freshness_diagnostics"

    def __init__(self, pool: AsyncConnectionPool):
        self._pool = pool

    async def __call__(self, object_name: str, schema: str = "public") -> dict:
        """
        Get detailed freshness diagnostics for a specific object, showing its freshness and
        the complete dependency chain with freshness information for each dependency.

        Args:
            object_name: Name of the object to analyze
            schema: Schema name (default: "public")

        Returns:
            Dictionary containing:
            - target_object: Information about the target object's freshness
            - dependency_chain: All dependencies with their individual freshness
            - freshness_summary: Summary of freshness issues in the chain
        """
        pool = self._pool
        result = {
            "target_object": None,
            "dependency_chain": [],
            "freshness_summary": {
                "total_dependencies": 0,
                "stale_dependencies": 0,
                "max_lag_seconds": 0,
                "critical_path_lag_seconds": 0,
            },
        }

        async with pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                # 1. Get target object information and freshness
                target_query = """
                SELECT 
                    f.object_id,
                    o.name as object_name,
                    s.name as schema_name,
                    o.type as object_type,
                    c.name as cluster_name,
                    f.write_frontier,
                    to_timestamp(f.write_frontier::text::numeric / 1000) as write_frontier_time,
                    EXTRACT(EPOCH FROM (mz_now()::timestamp - to_timestamp(f.write_frontier::text::numeric / 1000))) as lag_seconds
                FROM mz_objects o
                JOIN mz_schemas s ON o.schema_id = s.id
                LEFT JOIN mz_clusters c ON o.cluster_id = c.id
                LEFT JOIN mz_internal.mz_frontiers f ON o.id = f.object_id
                WHERE o.name = %s AND s.name = %s
                LIMIT 1
                """

                await cur.execute(target_query, [object_name, schema])
                target_row = await cur.fetchone()

                if not target_row:
                    return {
                        "error": f"Object '{object_name}' not found in schema '{schema}'"
                    }

                # Calculate lag in milliseconds
                lag_ms = (
                    target_row["lag_seconds"] * 1000 if target_row["lag_seconds"] else 0
                )

                result["target_object"] = {
                    "object_id": target_row["object_id"],
                    "object_name": target_row["object_name"],
                    "schema_name": target_row["schema_name"],
                    "object_type": target_row["object_type"],
                    "cluster_name": target_row["cluster_name"],
                    "write_frontier": target_row["write_frontier"],
                    "write_frontier_time": target_row["write_frontier_time"],
                    "lag_seconds": target_row["lag_seconds"],
                    "lag_ms": lag_ms,
                    "is_stale": lag_ms > 100,  # Consider >100ms as stale
                }

                target_object_id = target_row["object_id"]

                # 2. Get complete dependency chain with freshness for each dependency
                dependency_query = """
                WITH MUTUALLY RECURSIVE
                input_of (source text, target text) AS (
                    SELECT dependency_id, object_id 
                    FROM mz_internal.mz_compute_dependencies
                ),
                depends_on(prev text, next text, depth int) AS (
                    SELECT %s, %s, 0
                    UNION
                    SELECT input_of.source, depends_on.prev, depends_on.depth + 1
                    FROM input_of, depends_on
                    WHERE depends_on.prev = input_of.target
                    AND depends_on.depth < 10  -- Prevent infinite recursion
                )
                SELECT DISTINCT
                    depends_on.prev as dependency_id,
                    depends_on.next as dependent_id,
                    depends_on.depth,
                    dep_obj.name as dependency_name,
                    dep_obj.type as dependency_type,
                    dep_schema.name as dependency_schema,
                    dep_cluster.name as dependency_cluster,
                    dependent_obj.name as dependent_name,
                    dependent_obj.type as dependent_type,
                    dependent_schema.name as dependent_schema,
                    dependent_cluster.name as dependent_cluster,
                    dep_f.write_frontier as dependency_frontier,
                    to_timestamp(dep_f.write_frontier::text::numeric / 1000) as dependency_frontier_time,
                    EXTRACT(EPOCH FROM (mz_now()::timestamp - to_timestamp(dep_f.write_frontier::text::numeric / 1000))) as dependency_lag_seconds,
                    dependent_f.write_frontier as dependent_frontier,
                    to_timestamp(dependent_f.write_frontier::text::numeric / 1000) as dependent_frontier_time,
                    EXTRACT(EPOCH FROM (mz_now()::timestamp - to_timestamp(dependent_f.write_frontier::text::numeric / 1000))) as dependent_lag_seconds
                FROM depends_on
                LEFT JOIN mz_objects dep_obj ON depends_on.prev = dep_obj.id
                LEFT JOIN mz_schemas dep_schema ON dep_obj.schema_id = dep_schema.id
                LEFT JOIN mz_clusters dep_cluster ON dep_obj.cluster_id = dep_cluster.id
                LEFT JOIN mz_internal.mz_frontiers dep_f ON depends_on.prev = dep_f.object_id
                LEFT JOIN mz_objects dependent_obj ON depends_on.next = dependent_obj.id
                LEFT JOIN mz_schemas dependent_schema ON dependent_obj.schema_id = dependent_schema.id
                LEFT JOIN mz_clusters dependent_cluster ON dependent_obj.cluster_id = dependent_cluster.id
                LEFT JOIN mz_internal.mz_frontiers dependent_f ON depends_on.next = dependent_f.object_id
                WHERE depends_on.prev != depends_on.next
                ORDER BY depends_on.depth, depends_on.prev, depends_on.next
                """

                await cur.execute(
                    dependency_query, [target_object_id, target_object_id]
                )

                max_lag = 0
                stale_count = 0
                total_count = 0

                async for row in cur:
                    total_count += 1

                    # Calculate lag for dependency
                    dep_lag_seconds = row["dependency_lag_seconds"] or 0
                    dep_lag_ms = dep_lag_seconds * 1000
                    dependent_lag_seconds = row["dependent_lag_seconds"] or 0
                    dependent_lag_ms = dependent_lag_seconds * 1000

                    is_dep_stale = dep_lag_ms > 100
                    is_dependent_stale = dependent_lag_ms > 100

                    if is_dep_stale:
                        stale_count += 1

                    max_lag = max(max_lag, dep_lag_seconds, dependent_lag_seconds)

                    result["dependency_chain"].append(
                        {
                            "depth": row["depth"],
                            "dependency": {
                                "object_id": row["dependency_id"],
                                "name": row["dependency_name"],
                                "type": row["dependency_type"],
                                "schema": row["dependency_schema"],
                                "cluster": row["dependency_cluster"],
                                "write_frontier": row["dependency_frontier"],
                                "write_frontier_time": row["dependency_frontier_time"],
                                "lag_seconds": dep_lag_seconds,
                                "lag_ms": dep_lag_ms,
                                "is_stale": is_dep_stale,
                            },
                            "dependent": {
                                "object_id": row["dependent_id"],
                                "name": row["dependent_name"],
                                "type": row["dependent_type"],
                                "schema": row["dependent_schema"],
                                "cluster": row["dependent_cluster"],
                                "write_frontier": row["dependent_frontier"],
                                "write_frontier_time": row["dependent_frontier_time"],
                                "lag_seconds": dependent_lag_seconds,
                                "lag_ms": dependent_lag_ms,
                                "is_stale": is_dependent_stale,
                            },
                        }
                    )

                # Update summary
                result["freshness_summary"] = {
                    "total_dependencies": total_count,
                    "stale_dependencies": stale_count,
                    "max_lag_seconds": max_lag,
                    "critical_path_lag_seconds": max_lag,  # For now, use max lag as critical path
                }

        return result

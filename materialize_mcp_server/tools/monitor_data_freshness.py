from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool


class MonitorDataFreshness:
    __name__ = "monitor_data_freshness"

    def __init__(self, pool: AsyncConnectionPool):
        self._pool = pool

    async def __call__(
        self, threshold_seconds: float = 3.0, schema: str = None, cluster: str = None
    ) -> dict:
        """
        Monitor data freshness with dependency-aware analysis. Shows lagging objects and their dependency chains
        to help identify root causes of freshness issues.

        Args:
            threshold_seconds: Freshness threshold in seconds (supports fractional values, default: 3.0)
            schema: Optional schema name to filter objects
            cluster: Optional cluster name to filter objects

        Returns:
            Dictionary with three categories:
            - lagging_objects: Objects that exceed the freshness threshold
            - dependency_chains: Full dependency chains for each lagging object
            - critical_paths: Dependency edges that introduce delay, scored by lag amount
        """
        pool = self._pool
        result = {"lagging_objects": [], "dependency_chains": [], "critical_paths": []}

        async with pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                # 1. Get basic lagging objects info
                lagging_query = """
                SELECT 
                    f.object_id,
                    o.name as object_name,
                    s.name as schema_name,
                    o.type as object_type,
                    c.name as cluster_name,
                    f.write_frontier,
                    to_timestamp(f.write_frontier::text::numeric / 1000) as write_frontier_time,
                    EXTRACT(EPOCH FROM (mz_now()::timestamp - to_timestamp(f.write_frontier::text::numeric / 1000))) as lag_seconds
                FROM mz_internal.mz_frontiers f
                JOIN mz_objects o ON f.object_id = o.id
                JOIN mz_schemas s ON o.schema_id = s.id
                LEFT JOIN mz_clusters c ON o.cluster_id = c.id
                WHERE f.write_frontier > 0
                  AND mz_now() > to_timestamp(f.write_frontier::text::numeric / 1000) + INTERVAL '1 second' * %s
                  AND f.object_id LIKE 'u%%'
                """

                params = [threshold_seconds]
                if schema:
                    lagging_query += " AND s.name = %s"
                    params.append(schema)
                if cluster:
                    lagging_query += " AND c.name = %s"
                    params.append(cluster)

                lagging_query += " ORDER BY lag_seconds DESC"

                await cur.execute(lagging_query, params)
                async for row in cur:
                    lag_ms = row["lag_seconds"] * 1000 if row["lag_seconds"] else None
                    result["lagging_objects"].append(
                        {
                            "object_id": row["object_id"],
                            "object_name": row["object_name"],
                            "schema_name": row["schema_name"],
                            "object_type": row["object_type"],
                            "cluster_name": row["cluster_name"],
                            "write_frontier": row["write_frontier"],
                            "write_frontier_time": row["write_frontier_time"],
                            "lag_seconds": row["lag_seconds"],
                            "lag_ms": lag_ms,
                            "threshold_seconds": threshold_seconds,
                        }
                    )

                # 2. Get full dependency chains for lagging objects
                dependency_query = """
                WITH MUTUALLY RECURSIVE
                input_of (source text, target text) AS (
                    SELECT dependency_id, object_id 
                    FROM mz_internal.mz_compute_dependencies
                ),
                probes (id text) AS (
                    SELECT object_id
                    FROM mz_internal.mz_frontiers
                    WHERE write_frontier > 0
                      AND mz_now() > to_timestamp(write_frontier::text::numeric / 1000) + INTERVAL '1 second' * %s
                      AND object_id LIKE 'u%%'
                ),
                depends_on(probe text, prev text, next text) AS (
                    SELECT id, id, id FROM probes
                    UNION
                    SELECT depends_on.probe, input_of.source, input_of.target
                    FROM input_of, depends_on
                    WHERE depends_on.prev = input_of.target
                )
                SELECT 
                    depends_on.probe,
                    depends_on.prev as dependency_id,
                    depends_on.next as dependent_id,
                    prev_obj.name as dependency_name,
                    next_obj.name as dependent_name,
                    prev_obj.type as dependency_type,
                    next_obj.type as dependent_type
                FROM depends_on
                LEFT JOIN mz_objects prev_obj ON depends_on.prev = prev_obj.id
                LEFT JOIN mz_objects next_obj ON depends_on.next = next_obj.id
                WHERE probe != next
                ORDER BY probe, prev, next
                """

                await cur.execute(dependency_query, [threshold_seconds])
                async for row in cur:
                    result["dependency_chains"].append(
                        {
                            "probe_id": row["probe"],
                            "dependency_id": row["dependency_id"],
                            "dependent_id": row["dependent_id"],
                            "dependency_name": row["dependency_name"],
                            "dependent_name": row["dependent_name"],
                            "dependency_type": row["dependency_type"],
                            "dependent_type": row["dependent_type"],
                        }
                    )

                # 3. Get critical path analysis with lag scoring
                critical_path_query = """
                WITH MUTUALLY RECURSIVE
                input_of (source text, target text) AS (
                    SELECT dependency_id, object_id 
                    FROM mz_internal.mz_compute_dependencies
                ),
                probes (id text) AS (
                    SELECT object_id
                    FROM mz_internal.mz_frontiers
                    WHERE write_frontier > 0
                      AND mz_now() > to_timestamp(write_frontier::text::numeric / 1000) + INTERVAL '1 second' * %s
                      AND object_id LIKE 'u%%'
                ),
                depends_on(probe text, prev text, next text) AS (
                    SELECT id, id, id FROM probes
                    UNION
                    SELECT depends_on.probe, input_of.source, input_of.target
                    FROM input_of, depends_on
                    WHERE depends_on.prev = input_of.target
                )
                SELECT 
                    depends_on.probe,
                    depends_on.prev as source_id,
                    depends_on.next as target_id,
                    prev_obj.name as source_name,
                    next_obj.name as target_name,
                    prev_obj.type as source_type,
                    next_obj.type as target_type,
                    fp.write_frontier::text::numeric - fn.write_frontier::text::numeric as lag_ms,
                    (fp.write_frontier::text::numeric - fn.write_frontier::text::numeric) / 1000.0 as lag_seconds
                FROM depends_on, 
                     mz_internal.mz_frontiers fn, 
                     mz_internal.mz_frontiers fp
                LEFT JOIN mz_objects prev_obj ON depends_on.prev = prev_obj.id
                LEFT JOIN mz_objects next_obj ON depends_on.next = next_obj.id
                WHERE probe != prev
                  AND depends_on.next = fn.object_id
                  AND depends_on.prev = fp.object_id  
                  AND fp.write_frontier > fn.write_frontier
                ORDER BY lag_ms DESC
                """

                await cur.execute(critical_path_query, [threshold_seconds])
                async for row in cur:
                    result["critical_paths"].append(
                        {
                            "probe_id": row["probe"],
                            "source_id": row["source_id"],
                            "target_id": row["target_id"],
                            "source_name": row["source_name"],
                            "target_name": row["target_name"],
                            "source_type": row["source_type"],
                            "target_type": row["target_type"],
                            "lag_ms": row["lag_ms"],
                            "lag_seconds": row["lag_seconds"],
                        }
                    )

        return result

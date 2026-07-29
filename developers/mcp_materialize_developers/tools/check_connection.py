from typing import Any, Dict


class CheckConnection:
    __name__ = "check_connection"

    def __init__(self, pool):
        self._pool = pool

    async def __call__(self) -> Dict[str, Any]:
        """
        Check the connection status to Materialize database.

        This tool verifies if the MCP server can connect to Materialize and returns
        the connection state. This is useful for troubleshooting connection issues.

        Returns:
            Dictionary with connection status information including:
            - connected: Boolean indicating if connection is successful
            - state: Current connection state (unknown, connected, failed)
            - message: Human-readable status message
            - error: Error message if connection failed (null otherwise)
        """
        if self._pool is None:
            return {
                "connected": False,
                "state": "not_configured",
                "message": "Server is running in DOCS_ONLY mode - no database connection configured",
                "error": None,
            }

        is_healthy, message = await self._pool.check_health()

        return {
            "connected": is_healthy,
            "state": self._pool.state.value,
            "message": message,
            "error": self._pool.last_error if not is_healthy else None,
        }

# src/qi/config.py
from __future__ import annotations
import os
from dataclasses import dataclass

# Optional: load .env if present
try:
    from dotenv import load_dotenv
    load_dotenv()  # looks for .env in CWD / parents
except Exception:
    # dotenv is optional; ignore if not installed
    pass


def _getenv(*keys: str, default: str | None = None) -> str | None:
    """
    Return the first non-empty env var among keys, else default.
    Example: _getenv("CLICKHOUSE_HOST", "CH_HOST", default="localhost")
    """
    for k in keys:
        v = os.getenv(k)
        if v is not None and v != "":
            return v
    return default


@dataclass(frozen=True)
class ClickHouseConfig:
    host: str = _getenv("CLICKHOUSE_HOST", "CH_HOST", default="localhost")
    port: int = int(_getenv("CLICKHOUSE_PORT", "CH_PORT", default="8124") or 8124)  # HTTP port
    user: str = _getenv("CLICKHOUSE_USER", default="default") or "default"
    password: str = _getenv("CLICKHOUSE_PASSWORD", default="") or ""
    database: str = _getenv("CLICKHOUSE_DB", "CH_DB", default="market") or "market"
    secure: bool = (_getenv("CLICKHOUSE_SECURE", default="false") or "false").lower() in {"1", "true", "yes"}
    timeout: int = int(_getenv("CLICKHOUSE_TIMEOUT", default="10") or 10)

    @property
    def http_url(self) -> str:
        scheme = "https" if self.secure else "http"
        return f"{scheme}://{self.host}:{self.port}"

    def dsn(self) -> str:
        """Convenience string for logs."""
        auth = f"{self.user}@{self.host}:{self.port}"
        return f"clickhouse://{auth}/{self.database}{'?secure=1' if self.secure else ''}"


# Singleton-style access
CH = ClickHouseConfig()

# Optional: helper for clickhouse-connect
def make_clickhouse_client():
    """
    Returns a clickhouse_connect driver client using HTTP.
    Requires: pip install clickhouse-connect
    """
    import clickhouse_connect  # imported here so module import doesn't require the package
    return clickhouse_connect.get_client(
        host=CH.host,
        port=CH.port,
        username=CH.user,
        password=CH.password,
        database=CH.database,
        secure=CH.secure,
        connect_timeout=CH.timeout,
    )
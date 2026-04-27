"""VastDB connection configuration."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class VastDBConfig:
    """Configuration for connecting to VastDB.

    Can be constructed directly or from environment variables via
    :meth:`from_env`.

    Parameters
    ----------
    endpoint : str
        VastDB API endpoint (e.g. ``"http://vastdb.example.com:9090"``).
    access_key : str
        VastDB access key.
    secret_key : str
        VastDB secret key.
    bucket : str | None
        Default VastDB bucket name.  Optional — when omitted the bucket
        must be supplied as part of every table identifier passed to the
        catalog (``"bucket.schema.table"``).
    schema : str | None
        Default VastDB schema name within the bucket.  Optional — when
        omitted the schema must be supplied as part of every table
        identifier (``"bucket.schema.table"`` or ``"schema.table"`` when
        *bucket* is set).
    ssl_verify : bool
        Whether to verify SSL certificates. Default ``True``.
    """

    endpoint: str
    access_key: str
    secret_key: str
    bucket: str | None = None
    schema: str | None = None
    ssl_verify: bool = True

    def __post_init__(self) -> None:
        # Ensure endpoint has a scheme
        if not self.endpoint.startswith(("http://", "https://")):
            object.__setattr__(self, "endpoint", f"http://{self.endpoint}")

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------
    @classmethod
    def from_env(cls) -> VastDBConfig:
        """Build a :class:`VastDBConfig` from environment variables.

        Required env vars:
            ``VASTDB_ENDPOINT``, ``VASTDB_ACCESS_KEY``,
            ``VASTDB_SECRET_KEY``

        Optional env vars:
            ``VASTDB_BUCKET``, ``VASTDB_SCHEMA``,
            ``VASTDB_SSL_VERIFY`` (``"true"``/``"false"``)
        """

        def _require(name: str) -> str:
            val = os.environ.get(name)
            if val is None:
                raise OSError(f"Missing required environment variable: {name}")
            return val

        ssl_str = os.environ.get("VASTDB_SSL_VERIFY", "true").lower()

        return cls(
            endpoint=_require("VASTDB_ENDPOINT"),
            access_key=_require("VASTDB_ACCESS_KEY"),
            secret_key=_require("VASTDB_SECRET_KEY"),
            bucket=os.environ.get("VASTDB_BUCKET"),
            schema=os.environ.get("VASTDB_SCHEMA"),
            ssl_verify=ssl_str not in ("false", "0", "no"),
        )

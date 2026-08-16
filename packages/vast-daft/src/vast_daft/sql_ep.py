"""daft-sql-ep catalog plugin for VastDB.

Registered as entry point ``daft_sql_ep.catalogs:vastdb``. The EP package
does not import this module; it is loaded only when both packages are installed.
"""

from __future__ import annotations

import glob
import logging
import os
from typing import Any

from vast_daft.config import VastDBConfig
from vast_daft.table import VastDBCatalog

log = logging.getLogger(__name__)

HEADER_ENDPOINT = "x-daft-vastdb-endpoint"
HEADER_BUCKET = "x-daft-bucket"
HEADER_SCHEMA = "x-daft-schema"


class VastDBProvider:
    """Per-call VastDB catalog from Flight Basic auth. No process-env S3 keys."""

    name = "vastdb"

    def shared(self) -> tuple[str, Any] | None:
        return None

    def for_call(self, auth: Any) -> tuple[str, Any] | None:
        username = getattr(auth, "username", None) or getattr(auth, "access_key", None)
        password = getattr(auth, "password", None) or getattr(auth, "secret_key", None)
        if not username or not password:
            return None
        endpoint = _auth_header(auth, HEADER_ENDPOINT, "x-daft-endpoint") or os.environ.get("VASTDB_ENDPOINT")
        if not endpoint:
            raise RuntimeError("VastDB endpoint missing: send x-daft-vastdb-endpoint or set VASTDB_ENDPOINT")
        alias = os.environ.get("DAFT_SQL_EP_VASTDB_ALIAS", "vastdb")
        config = VastDBConfig(
            endpoint=endpoint,
            access_key=username,
            secret_key=password,
            bucket=_auth_header(auth, HEADER_BUCKET) or os.environ.get("VASTDB_BUCKET"),
            schema=_auth_header(auth, HEADER_SCHEMA) or os.environ.get("VASTDB_SCHEMA"),
            ssl_verify=os.environ.get("VASTDB_SSL_VERIFY", "false").lower() not in {"false", "0", "no"},
        )
        log.info(
            "VastDB catalog alias=%s endpoint=%s bucket=%s schema=%s",
            alias,
            config.endpoint,
            config.bucket,
            config.schema,
        )
        return alias, VastDBCatalog(config, alias=alias)

    def cache_key(self, auth: Any) -> tuple[Any, ...] | None:
        username = getattr(auth, "username", None) or getattr(auth, "access_key", None)
        password = getattr(auth, "password", None) or getattr(auth, "secret_key", None)
        if not username or not password:
            return None
        return (
            username,
            password,
            _auth_header(auth, HEADER_ENDPOINT, "x-daft-endpoint"),
            _auth_header(auth, HEADER_BUCKET),
            _auth_header(auth, HEADER_SCHEMA),
        )

    def runtime_env(self) -> dict[str, Any]:
        if os.environ.get("VAST_DAFT_SOURCE") == "pypi":
            package = os.environ.get("VAST_DAFT_PYPI_PACKAGE", "vast-daft")
            version = os.environ.get("VAST_DAFT_PYPI_VERSION", "").strip()
            spec = f"{package}=={version}" if version else package
            return {"pip": [spec, "vastdb>=1.2", "numpy<2", "tenacity>=8.0"]}
        wheels = sorted(glob.glob("/mnt/wheel/vast_daft-*.whl"))
        extra = os.environ.get("VAST_DAFT_WHEEL")
        if extra and os.path.isfile(extra):
            wheels = [extra]
        if not wheels:
            return {"pip": ["vastdb>=1.2", "numpy<2", "tenacity>=8.0"]}
        return {
            "py_modules": wheels,
            "pip": ["vastdb>=1.2", "numpy<2", "tenacity>=8.0"],
        }


def _auth_header(auth: Any, *names: str) -> str | None:
    header = getattr(auth, "header", None)
    if callable(header):
        return header(*names)
    headers = getattr(auth, "headers", {}) or {}
    for name in names:
        value = headers.get(name) or headers.get(name.lower())
        if value:
            return str(value)
    return None

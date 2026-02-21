from __future__ import annotations

import builtins
import os
from collections.abc import Mapping

from src.common.config_loader import get_config_value

DEFAULT_SECRET_SCOPE = get_config_value("analytics.secret_scope")
DEFAULT_SECRET_KEY = get_config_value("analytics.secret_key")
LOCAL_DUMMY_SALT = "local-salt-v1"
ENV_ANON_SALT = "ANON_USER_KEY_SALT"
ENV_DATABRICKS_RUNTIME_VERSION = "DATABRICKS_RUNTIME_VERSION"


def _is_databricks_runtime(env_map: Mapping[str, str]) -> bool:
    # Databricks sets runtime env vars on clusters; notebooks also provide a
    # global dbutils object. Prefer fail-closed behavior in those contexts.
    if env_map.get(ENV_DATABRICKS_RUNTIME_VERSION):
        return True
    return getattr(builtins, "dbutils", None) is not None


def _try_get_dbutils():
    candidate = getattr(builtins, "dbutils", None)
    if candidate is not None:
        return candidate

    try:
        from pyspark.dbutils import DBUtils
        from pyspark.sql import SparkSession
    except Exception:
        return None

    spark = SparkSession.getActiveSession()
    if spark is None:
        return None
    try:
        return DBUtils(spark)
    except Exception:
        return None


def resolve_user_key_salt(
    *,
    secret_scope: str = DEFAULT_SECRET_SCOPE,
    secret_key: str = DEFAULT_SECRET_KEY,
    env: Mapping[str, str] | None = None,
    dbutils=None,
    allow_local_fallback: bool = True,
) -> str:
    env_map = env if env is not None else os.environ
    in_databricks = _is_databricks_runtime(env_map)
    env_salt = env_map.get(ENV_ANON_SALT)
    if env_salt:
        return env_salt

    client = dbutils if dbutils is not None else _try_get_dbutils()
    if client is not None:
        try:
            secret_value = client.secrets.get(scope=secret_scope, key=secret_key)
        except Exception:
            secret_value = None
        if secret_value:
            return secret_value

    if allow_local_fallback:
        if not in_databricks:
            return LOCAL_DUMMY_SALT

    raise RuntimeError(
        "Unable to resolve anonymization salt from env or Databricks secret scope "
        f"(scope={secret_scope}, key={secret_key})"
    )

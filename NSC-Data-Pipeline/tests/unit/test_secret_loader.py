from __future__ import annotations

import pytest

from src.common.config_loader import get_config_value
from src.io.secret_loader import (
    DEFAULT_SECRET_KEY,
    DEFAULT_SECRET_SCOPE,
    LOCAL_DUMMY_SALT,
    resolve_user_key_salt,
)


class _SecretClient:
    def __init__(self, value: str | None = None, *, raise_error: bool = False) -> None:
        self._value = value
        self._raise_error = raise_error

    def get(self, *, scope: str, key: str) -> str | None:
        if self._raise_error:
            raise RuntimeError("secret lookup failed")
        assert scope
        assert key
        return self._value


class _DbutilsStub:
    def __init__(self, value: str | None = None, *, raise_error: bool = False) -> None:
        self.secrets = _SecretClient(value, raise_error=raise_error)


def test_default_secret_scope_and_key_are_from_config() -> None:
    assert DEFAULT_SECRET_SCOPE == get_config_value("analytics.secret_scope")
    assert DEFAULT_SECRET_KEY == get_config_value("analytics.secret_key")


def test_resolve_user_key_salt_prefers_env() -> None:
    salt = resolve_user_key_salt(
        env={"ANON_USER_KEY_SALT": "salt-from-env"},
        dbutils=_DbutilsStub("salt-from-secret"),
    )
    assert salt == "salt-from-env"


def test_resolve_user_key_salt_reads_databricks_secret() -> None:
    salt = resolve_user_key_salt(
        env={},
        dbutils=_DbutilsStub("salt-from-secret"),
    )
    assert salt == "salt-from-secret"


def test_resolve_user_key_salt_uses_local_dummy_fallback() -> None:
    salt = resolve_user_key_salt(
        env={},
        dbutils=_DbutilsStub(raise_error=True),
    )
    assert salt == LOCAL_DUMMY_SALT


def test_resolve_user_key_salt_raises_without_fallback() -> None:
    with pytest.raises(RuntimeError):
        resolve_user_key_salt(
            env={},
            dbutils=_DbutilsStub(raise_error=True),
            allow_local_fallback=False,
        )


def test_resolve_user_key_salt_fails_closed_in_databricks_runtime() -> None:
    with pytest.raises(RuntimeError):
        resolve_user_key_salt(
            env={"DATABRICKS_RUNTIME_VERSION": "13.3"},
            dbutils=_DbutilsStub(raise_error=True),
        )

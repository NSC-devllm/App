from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from typing import Iterable, Mapping
from urllib.parse import parse_qsl, urlencode

import jwt
from jwt import PyJWKClient

from src.api.constants import ADMIN_AUDIT_ROLE, ADMIN_READ_ROLE
from src.common.config import load_config

try:
    from starlette.requests import Request  # type: ignore
except Exception:  # pragma: no cover - fallback for unit tests without deps
    Request = object  # type: ignore


auth_required_roles = {
    ADMIN_READ_ROLE,
    ADMIN_AUDIT_ROLE,
}


@dataclass(frozen=True)
class ActorContext:
    actor_id: str | None
    actor_roles: list[str]


class AuthError(Exception):
    pass


def _split_csv(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def _normalize_roles(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [item for item in value.split() if item]
    if isinstance(value, (list, tuple, set)):
        return [str(item) for item in value if str(item)]
    return []


def extract_roles(
    claims: Mapping[str, object], role_claims: Iterable[str]
) -> list[str]:
    roles: list[str] = []
    for claim in role_claims:
        raw = claims.get(claim)
        roles.extend(_normalize_roles(raw))
    # preserve order but unique
    seen = set()
    ordered = []
    for role in roles:
        if role in seen:
            continue
        seen.add(role)
        ordered.append(role)
    return ordered


def extract_actor_id(
    claims: Mapping[str, object], actor_claims: Iterable[str]
) -> str | None:
    for claim in actor_claims:
        value = claims.get(claim)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def mask_query(query: str | None, sensitive_keys: Iterable[str]) -> str | None:
    if not query:
        return None
    sensitive = {key.lower() for key in sensitive_keys}
    pairs = parse_qsl(query, keep_blank_values=True)
    masked = []
    for key, value in pairs:
        if key.lower() in sensitive:
            masked.append((key, "***"))
        else:
            masked.append((key, value))
    return urlencode(masked)


@lru_cache(maxsize=1)
def _get_jwks_client(jwks_url: str) -> PyJWKClient:
    return PyJWKClient(jwks_url)


def _decode_token(token: str) -> dict:
    config = load_config()
    if not config.auth_issuer or not config.auth_audience or not config.auth_jwks_url:
        raise AuthError("OIDC settings missing")

    jwks_client = _get_jwks_client(config.auth_jwks_url)
    signing_key = jwks_client.get_signing_key_from_jwt(token)

    return jwt.decode(
        token,
        signing_key.key,
        algorithms=[config.auth_algorithm],
        audience=config.auth_audience,
        issuer=config.auth_issuer,
    )


def _extract_bearer(request: Request) -> str:
    auth_header = request.headers.get("authorization")
    if not auth_header:
        raise AuthError("Authorization header missing")
    parts = auth_header.split()
    if len(parts) != 2 or parts[0].lower() != "bearer":
        raise AuthError("Authorization header must be Bearer token")
    return parts[1]


def require_admin_read(request: Request) -> ActorContext:
    from fastapi import HTTPException

    config = load_config()
    if config.auth_mode == "disabled":
        actor_id = request.headers.get("X-Actor-Id")
        actor_roles = _split_csv(request.headers.get("X-Actor-Roles"))
        request.state.actor_id = actor_id
        request.state.actor_roles = actor_roles
        return ActorContext(actor_id=actor_id, actor_roles=actor_roles)

    try:
        token = _extract_bearer(request)
        claims = _decode_token(token)
    except AuthError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc
    except jwt.PyJWTError as exc:
        raise HTTPException(status_code=401, detail="Invalid token") from exc

    role_claims = _split_csv(config.auth_roles_claim)
    actor_claims = _split_csv(config.auth_actor_id_claims)

    roles = extract_roles(claims, role_claims or ["roles"])  # fallback
    actor_id = extract_actor_id(claims, actor_claims or ["sub"])

    if not set(roles).intersection(auth_required_roles):
        raise HTTPException(status_code=403, detail="Forbidden")

    request.state.actor_id = actor_id
    request.state.actor_roles = roles
    return ActorContext(actor_id=actor_id, actor_roles=roles)

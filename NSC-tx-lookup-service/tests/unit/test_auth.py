from __future__ import annotations

from src.api.auth import (
    _normalize_roles,
    _split_csv,
    auth_required_roles,
    extract_actor_id,
    extract_roles,
    mask_query,
)


def test_extract_roles_combines_sources() -> None:
    claims = {
        "roles": ["ADMIN_READ"],
        "scp": "ADMIN_AUDIT OTHER",
    }
    roles = extract_roles(claims, ["roles", "scp"])
    assert "ADMIN_READ" in roles
    assert "ADMIN_AUDIT" in roles


def test_extract_roles_deduplicates() -> None:
    claims = {"roles": ["ADMIN_READ", "ADMIN_READ"]}
    roles = extract_roles(claims, ["roles"])
    assert roles == ["ADMIN_READ"]


def test_extract_roles_empty_claims() -> None:
    roles = extract_roles({}, ["roles", "scp"])
    assert roles == []


def test_extract_actor_id_prefers_first_match() -> None:
    claims = {"sub": "user-1", "email": "user@example.com"}
    actor = extract_actor_id(claims, ["sub", "email"])
    assert actor == "user-1"


def test_extract_actor_id_skips_empty() -> None:
    claims = {"sub": "", "email": "user@example.com"}
    actor = extract_actor_id(claims, ["sub", "email"])
    assert actor == "user@example.com"


def test_extract_actor_id_returns_none() -> None:
    claims = {"unrelated": "value"}
    actor = extract_actor_id(claims, ["sub", "email"])
    assert actor is None


def test_mask_query_hides_sensitive_keys() -> None:
    query = "token=secret&foo=bar&access_token=top"
    masked = mask_query(query, ["token", "access_token"])
    assert masked is not None
    assert "token=%2A%2A%2A" in masked
    assert "access_token=%2A%2A%2A" in masked
    assert "foo=bar" in masked


def test_mask_query_returns_none_for_empty() -> None:
    assert mask_query(None, ["token"]) is None
    assert mask_query("", ["token"]) is None


def test_split_csv_basic() -> None:
    assert _split_csv("a,b,c") == ["a", "b", "c"]
    assert _split_csv(" a , b ") == ["a", "b"]


def test_split_csv_empty() -> None:
    assert _split_csv(None) == []
    assert _split_csv("") == []


def test_normalize_roles_with_tuple() -> None:
    assert _normalize_roles(("ADMIN_READ", "ADMIN_AUDIT")) == [
        "ADMIN_READ",
        "ADMIN_AUDIT",
    ]


def test_normalize_roles_with_set() -> None:
    result = _normalize_roles({"ADMIN_READ"})
    assert "ADMIN_READ" in result


def test_normalize_roles_none_returns_empty() -> None:
    assert _normalize_roles(None) == []


def test_normalize_roles_non_iterable_returns_empty() -> None:
    assert _normalize_roles(12345) == []


def test_auth_required_roles_includes_admin_audit() -> None:
    assert "ADMIN_READ" in auth_required_roles
    assert "ADMIN_AUDIT" in auth_required_roles


def test_auth_required_roles_rejects_unknown_role() -> None:
    unknown_roles = {"VIEWER", "SUPPORT_AGENT"}
    assert not unknown_roles.intersection(auth_required_roles)

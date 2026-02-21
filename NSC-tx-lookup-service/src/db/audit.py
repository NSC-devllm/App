from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from src.db.models import AdminAuditLog


def record_admin_audit(session: Session, fields: dict[str, Any]) -> None:
    session.add(AdminAuditLog(**fields))

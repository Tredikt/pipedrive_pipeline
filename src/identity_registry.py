"""
Сквозная таблица master.person_identity (sql/021 + 024).

Правило JIRA: новая строка создаётся только из PeopleForce (upsert_person_identity_from_peopleforce).
Pipedrive (и Jira webhook) только merge_person_identity_from_crm по email;
при отсутствии совпадения — master.identity_link_pending + опционально HTTP-алерт.
"""

from __future__ import annotations

import logging
import os
from typing import Any

import psycopg
from psycopg.types.json import Json

logger = logging.getLogger(__name__)


def _clean_email(email: str | None) -> str | None:
    if email is None:
        return None
    s = str(email).strip()
    return s or None


def _clean_text(x: Any) -> str | None:
    if x is None:
        return None
    s = str(x).strip()
    return s or None


def upsert_person_identity_from_peopleforce(
    cur: psycopg.Cursor,
    *,
    email: str | None,
    full_name: str | None = None,
    first_name: str | None = None,
    last_name: str | None = None,
    peopleforce_employee_id: int | None = None,
    pipedrive_person_id: int | None = None,
    pipedrive_user_id: int | None = None,
    jira_id: str | None = None,
    google_analytics_id: str | None = None,
) -> None:
    """INSERT или UPDATE по email; из PF допускается создание строки (источник правды)."""
    em = _clean_email(email)
    if em is None:
        return
    cur.execute(
        """
        INSERT INTO master.person_identity (
            email, full_name, first_name, last_name,
            pipedrive_person_id, pipedrive_user_id,
            peopleforce_employee_id, jira_id, google_analytics_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (email_norm) DO UPDATE SET
            full_name = COALESCE(
                NULLIF(TRIM(EXCLUDED.full_name), ''),
                master.person_identity.full_name
            ),
            first_name = COALESCE(
                NULLIF(TRIM(EXCLUDED.first_name), ''),
                master.person_identity.first_name
            ),
            last_name = COALESCE(
                NULLIF(TRIM(EXCLUDED.last_name), ''),
                master.person_identity.last_name
            ),
            pipedrive_person_id = COALESCE(
                EXCLUDED.pipedrive_person_id,
                master.person_identity.pipedrive_person_id
            ),
            pipedrive_user_id = COALESCE(
                EXCLUDED.pipedrive_user_id,
                master.person_identity.pipedrive_user_id
            ),
            peopleforce_employee_id = COALESCE(
                EXCLUDED.peopleforce_employee_id,
                master.person_identity.peopleforce_employee_id
            ),
            jira_id = COALESCE(EXCLUDED.jira_id, master.person_identity.jira_id),
            google_analytics_id = COALESCE(
                EXCLUDED.google_analytics_id,
                master.person_identity.google_analytics_id
            )
        """,
        (
            em,
            _clean_text(full_name),
            _clean_text(first_name),
            _clean_text(last_name),
            pipedrive_person_id,
            pipedrive_user_id,
            peopleforce_employee_id,
            _clean_text(jira_id),
            _clean_text(google_analytics_id),
        ),
    )


def merge_person_identity_from_crm(
    cur: psycopg.Cursor,
    *,
    email: str | None,
    pipedrive_person_id: int | None = None,
    pipedrive_user_id: int | None = None,
    jira_id: str | None = None,
    full_name_hint: str | None = None,
) -> bool:
    """
    Только UPDATE существующей строки по email.
    CRM не создаёт записи. Возвращает True, если строка найдена и обновлена.
    """
    em = _clean_email(email)
    if em is None:
        return False
    jid = _clean_text(jira_id)
    cur.execute(
        """
        UPDATE master.person_identity SET
            pipedrive_person_id = COALESCE(%s, pipedrive_person_id),
            pipedrive_user_id = COALESCE(%s, pipedrive_user_id),
            jira_id = COALESCE(NULLIF(trim(%s), ''), jira_id),
            full_name = COALESCE(
                NULLIF(trim(%s), ''),
                full_name
            )
        WHERE email_norm = lower(trim(%s))
        """,
        (
            pipedrive_person_id,
            pipedrive_user_id,
            jid or "",
            _clean_text(full_name_hint) or "",
            em,
        ),
    )
    return cur.rowcount > 0


def record_identity_link_pending(
    cur: psycopg.Cursor,
    *,
    source_system: str,
    entity_kind: str,
    entity_id: int,
    email: str | None,
    detail: str | None = None,
    payload: dict[str, Any] | None = None,
) -> None:
    """Зафиксировать событие без совпадения по email (идемпотентно по системе/kind/id)."""
    em = _clean_email(email)
    if em is None:
        em = ""
    cur.execute(
        """
        INSERT INTO master.identity_link_pending (
            source_system, entity_kind, entity_id, email, detail, payload
        ) VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (source_system, entity_kind, entity_id) DO UPDATE SET
            email = EXCLUDED.email,
            detail = EXCLUDED.detail,
            payload = EXCLUDED.payload,
            created_at = NOW()
        """,
        (
            source_system.strip().lower(),
            entity_kind.strip().lower(),
            entity_id,
            em,
            detail,
            Json(payload) if payload else None,
        ),
    )


def notify_identity_match_miss(
    *,
    source_system: str,
    entity_kind: str,
    entity_id: int,
    email: str | None,
    detail: str | None = None,
) -> None:
    """Опционально POST на HR_MATCH_ALERT_WEBHOOK_URL (Slack Incoming Webhook и т.п.)."""
    url = os.environ.get("HR_MATCH_ALERT_WEBHOOK_URL", "").strip()
    if not url:
        return
    text = (
        f"[identity] Нет строки person_identity для email из {source_system}: "
        f"{entity_kind} id={entity_id}, email={email or '(пусто)'}. "
        f"Проверьте email в CRM или добавьте сотрудника в PeopleForce. "
        f"{detail or ''}".strip()
    )
    body = {
        "text": text,
        "reason": "person_identity_miss",
        "source_system": source_system,
        "entity_kind": entity_kind,
        "entity_id": entity_id,
        "email": email,
    }
    try:
        import httpx

        r = httpx.post(url, json=body, timeout=15.0)
        r.raise_for_status()
    except Exception:
        logger.exception("HR_MATCH_ALERT_WEBHOOK_URL notify failed")


# Совместимость со скриптами: прежнее имя = только PF / административный upsert
def upsert_person_identity_row(
    cur: psycopg.Cursor,
    *,
    email: str | None,
    full_name: str | None = None,
    pipedrive_person_id: int | None = None,
    pipedrive_user_id: int | None = None,
    peopleforce_employee_id: int | None = None,
    jira_id: str | None = None,
    google_analytics_id: str | None = None,
    first_name: str | None = None,
    last_name: str | None = None,
) -> None:
    upsert_person_identity_from_peopleforce(
        cur,
        email=email,
        full_name=full_name,
        first_name=first_name,
        last_name=last_name,
        pipedrive_person_id=pipedrive_person_id,
        pipedrive_user_id=pipedrive_user_id,
        peopleforce_employee_id=peopleforce_employee_id,
        jira_id=jira_id,
        google_analytics_id=google_analytics_id,
    )

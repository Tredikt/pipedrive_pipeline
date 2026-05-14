"""
Связка master.hr_employee и master.person_identity после webhook-upsert.

PeopleForce — источник правды: создаёт/обновляет строки person_identity (имя, PF id).
Pipedrive только merge по email (pipedrive_* и подсказка jira из HR); без совпадения —
master.identity_link_pending + опционально HR_MATCH_ALERT_WEBHOOK_URL.
"""

from __future__ import annotations

from typing import Any

import psycopg

from src.identity_registry import (
    merge_person_identity_from_crm,
    notify_identity_match_miss,
    record_identity_link_pending,
    upsert_person_identity_from_peopleforce,
)


def _hr_jira_for_email(cur: psycopg.Cursor, email: str | None) -> str | None:
    if not email or not str(email).strip():
        return None
    cur.execute(
        """
        SELECT jira_id FROM master.hr_employee
        WHERE lower(trim(email)) = lower(trim(%s))
          AND jira_id IS NOT NULL AND trim(jira_id) <> ''
        LIMIT 1
        """,
        (email.strip(),),
    )
    row = cur.fetchone()
    return str(row[0]).strip() if row and row[0] else None


def _pipedrive_miss(
    cur: psycopg.Cursor,
    *,
    entity_kind: str,
    entity_id: int,
    email: str | None,
    detail: str,
) -> None:
    record_identity_link_pending(
        cur,
        source_system="pipedrive",
        entity_kind=entity_kind,
        entity_id=entity_id,
        email=email,
        detail=detail,
        payload=None,
    )
    notify_identity_match_miss(
        source_system="pipedrive",
        entity_kind=entity_kind,
        entity_id=entity_id,
        email=email,
        detail=detail,
    )


def link_master_after_pipedrive_upsert(
    cur: psycopg.Cursor, spec_name: str, entity_id: int
) -> None:
    """Обновить master.hr_employee и person_identity после успешной записи в pipedrive_dm."""
    spec = (spec_name or "").strip().lower()
    if spec == "users":
        cur.execute(
            """
            UPDATE master.hr_employee h
            SET pipedrive_user_id = %s
            FROM pipedrive_dm.pipedrive_user u
            WHERE u.id = %s
              AND h.email IS NOT NULL AND trim(h.email) <> ''
              AND u.email IS NOT NULL AND trim(u.email) <> ''
              AND lower(trim(h.email)) = lower(trim(u.email))
            """,
            (entity_id, entity_id),
        )
        cur.execute(
            "SELECT email, name FROM pipedrive_dm.pipedrive_user WHERE id = %s",
            (entity_id,),
        )
        row = cur.fetchone()
        if not row or not row[0]:
            return
        em, nm = row[0], row[1]
        em_s = str(em).strip()
        jid = _hr_jira_for_email(cur, em_s)
        merged = merge_person_identity_from_crm(
            cur,
            email=em_s,
            pipedrive_user_id=entity_id,
            jira_id=jid,
            full_name_hint=str(nm).strip() if nm else None,
        )
        if not merged:
            _pipedrive_miss(
                cur,
                entity_kind="user",
                entity_id=entity_id,
                email=em_s,
                detail="Нет person_identity по этому email (ожидается запись из PeopleForce).",
            )
        return

    if spec == "persons":
        cur.execute(
            """
            UPDATE master.hr_employee h
            SET pipedrive_person_id = %s
            FROM pipedrive_dm.person p
            WHERE p.id = %s
              AND h.email IS NOT NULL AND trim(h.email) <> ''
              AND p.primary_email IS NOT NULL AND trim(p.primary_email) <> ''
              AND lower(trim(h.email)) = lower(trim(p.primary_email))
            """,
            (entity_id, entity_id),
        )
        cur.execute(
            """
            SELECT primary_email,
                   COALESCE(
                       NULLIF(trim(name), ''),
                       trim(both ' ' FROM coalesce(first_name, '') || ' ' || coalesce(last_name, ''))
                   )
            FROM pipedrive_dm.person WHERE id = %s
            """,
            (entity_id,),
        )
        row = cur.fetchone()
        if not row or not row[0]:
            return
        em, nm = row[0], row[1]
        em_s = str(em).strip()
        jid = _hr_jira_for_email(cur, em_s)
        merged = merge_person_identity_from_crm(
            cur,
            email=em_s,
            pipedrive_person_id=entity_id,
            jira_id=jid,
            full_name_hint=str(nm).strip() if nm else None,
        )
        if not merged:
            _pipedrive_miss(
                cur,
                entity_kind="person",
                entity_id=entity_id,
                email=em_s,
                detail="Нет person_identity по этому email (ожидается запись из PeopleForce).",
            )


def link_master_after_pf_employee_upsert(
    cur: psycopg.Cursor, employee_id: int, flat: dict[str, Any]
) -> None:
    """Проставить master.hr_employee.pf_id по email и создать/обновить person_identity."""
    email = flat.get("email")
    if email is None:
        return
    em = str(email).strip().lower()
    if not em:
        return
    cur.execute(
        """
        UPDATE master.hr_employee
        SET pf_id = %s
        WHERE lower(trim(email)) = %s
        """,
        (employee_id, em),
    )
    fn = flat.get("full_name")
    fn_raw = str(fn).strip() if fn else None
    first = flat.get("first_name")
    last = flat.get("last_name")
    upsert_person_identity_from_peopleforce(
        cur,
        email=str(email).strip(),
        full_name=fn_raw,
        first_name=str(first).strip() if first else None,
        last_name=str(last).strip() if last else None,
        peopleforce_employee_id=employee_id,
        jira_id=_hr_jira_for_email(cur, str(email)),
    )

"""
Сквозная таблица master.person_identity: слияние по email (sql/021_master_person_identity.sql).

Webhook и синки дополняют непустые поля; пустые в событии не затирают уже сохранённые значения.
"""

from __future__ import annotations

from typing import Any

import psycopg


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
) -> None:
    """INSERT или UPDATE строки по lower(trim(email)); None не перетирает существующие id."""
    em = _clean_email(email)
    if em is None:
        return
    cur.execute(
        """
        INSERT INTO master.person_identity (
            email, full_name, pipedrive_person_id, pipedrive_user_id,
            peopleforce_employee_id, jira_id, google_analytics_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (email_norm) DO UPDATE SET
            full_name = COALESCE(
                NULLIF(TRIM(EXCLUDED.full_name), ''),
                master.person_identity.full_name
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
            pipedrive_person_id,
            pipedrive_user_id,
            peopleforce_employee_id,
            _clean_text(jira_id),
            _clean_text(google_analytics_id),
        ),
    )

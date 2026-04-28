"""
Связка master.hr_employee с витринами после webhook-upsert (инкрементально по email / id).

Правило: Pipedrive дописывает pipedrive_* у уже существующих строк master по совпадению email;
PeopleForce проставляет pf_id там же.
"""

from __future__ import annotations

from typing import Any

import psycopg


def link_master_after_pipedrive_upsert(
    cur: psycopg.Cursor, spec_name: str, entity_id: int
) -> None:
    """Обновить master.hr_employee после успешной записи сущности в pipedrive_dm."""
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


def link_master_after_pf_employee_upsert(
    cur: psycopg.Cursor, employee_id: int, flat: dict[str, Any]
) -> None:
    """Проставить master.hr_employee.pf_id по email после upsert в peopleforce_dm.employee."""
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

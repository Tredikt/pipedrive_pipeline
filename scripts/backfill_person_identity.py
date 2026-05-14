"""
Заполнить master.person_identity из PeopleForce и HR master.

Порядок (правило JIRA):
  1) peopleforce_dm.employee — создаёт/обновляет строки person_identity (источник правды).
  2) master.hr_employee — только merge существующих строк по email (pipedrive/jira id),
     без создания новых записей из HR-only email.

  python scripts/backfill_person_identity.py

Нужны DATABASE_URL и миграции sql/021_master_person_identity.sql и sql/024_master_identity_pf_rules.sql.
"""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv

load_dotenv()

from src.config import get_database_url
from src.db import connect
from src.identity_registry import merge_person_identity_from_crm, upsert_person_identity_row


def main() -> None:
    with connect(get_database_url()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, email, full_name, first_name, last_name
                FROM peopleforce_dm.employee
                WHERE email IS NOT NULL AND trim(email) <> ''
                """
            )
            n_pf = 0
            for row in cur.fetchall():
                eid, em, fn, fi, la = row
                upsert_person_identity_row(
                    cur,
                    email=str(em),
                    full_name=str(fn).strip() if fn else None,
                    first_name=str(fi).strip() if fi else None,
                    last_name=str(la).strip() if la else None,
                    peopleforce_employee_id=int(eid),
                )
                n_pf += 1

            cur.execute(
                """
                SELECT email, pf_full_name, pipedrive_user_id, pipedrive_person_id,
                       pf_id, jira_id
                FROM master.hr_employee
                WHERE email IS NOT NULL AND trim(email) <> ''
                """
            )
            n_hr_merged = 0
            n_hr_skip = 0
            for row in cur.fetchall():
                em, pfn, pu, pp, pfid, jid = row
                merged = merge_person_identity_from_crm(
                    cur,
                    email=str(em),
                    pipedrive_user_id=int(pu) if pu is not None else None,
                    pipedrive_person_id=int(pp) if pp is not None else None,
                    jira_id=str(jid).strip() if jid else None,
                    full_name_hint=str(pfn).strip() if pfn else None,
                )
                if merged:
                    n_hr_merged += 1
                else:
                    n_hr_skip += 1

        conn.commit()

    print(
        f"person_identity: PF создано/обновлено {n_pf} строк; "
        f"HR merge применён к {n_hr_merged} (пропуск без строки PF — {n_hr_skip}).",
        flush=True,
    )


if __name__ == "__main__":
    main()

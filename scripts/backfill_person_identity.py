"""
Заполнить master.person_identity из master.hr_employee и peopleforce_dm.employee.

Первый проход — строки HR master (email + все известные id).
Второй — сотрудники PF (дописать pf и имя, если ещё не склеено по email).

  python scripts/backfill_person_identity.py

Нужны DATABASE_URL и применённый sql/021_master_person_identity.sql.
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
from src.identity_registry import upsert_person_identity_row


def main() -> None:
    with connect(get_database_url()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT email, pf_full_name, pipedrive_user_id, pipedrive_person_id,
                       pf_id, jira_id
                FROM master.hr_employee
                WHERE email IS NOT NULL AND trim(email) <> ''
                """
            )
            n_hr = 0
            for row in cur.fetchall():
                em, pfn, pu, pp, pfid, jid = row
                upsert_person_identity_row(
                    cur,
                    email=str(em),
                    full_name=str(pfn).strip() if pfn else None,
                    pipedrive_user_id=int(pu) if pu is not None else None,
                    pipedrive_person_id=int(pp) if pp is not None else None,
                    peopleforce_employee_id=int(pfid) if pfid is not None else None,
                    jira_id=str(jid).strip() if jid else None,
                )
                n_hr += 1

            cur.execute(
                """
                SELECT id, email, full_name FROM peopleforce_dm.employee
                WHERE email IS NOT NULL AND trim(email) <> ''
                """
            )
            n_pf = 0
            for row in cur.fetchall():
                eid, em, fn = row
                upsert_person_identity_row(
                    cur,
                    email=str(em),
                    full_name=str(fn).strip() if fn else None,
                    peopleforce_employee_id=int(eid),
                )
                n_pf += 1

        conn.commit()

    print(
        f"person_identity: из HR обработано {n_hr} строк, проход PF — {n_pf} строк.",
        flush=True,
    )


if __name__ == "__main__":
    main()

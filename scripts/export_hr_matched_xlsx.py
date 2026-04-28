"""
Один лист Excel: найденные совпадения HR master ↔ PeopleForce / Pipedrive.

Колонка source — строкой «PeopleForce» или «Pipedrive»; entity уточняет тип строки.

Критерии те же, что были отдельными листами раньше:
  • PeopleForce — JOIN по pf_id
  • Pipedrive — user или person, совпадение по email с master.email

  python scripts/export_hr_matched_xlsx.py
  python scripts/export_hr_matched_xlsx.py -o files/hr_matched.xlsx
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv

load_dotenv()

from openpyxl import Workbook

from src.config import get_database_url
from src.db import connect
from src.excel_openpyxl_value import cell_value_for_openpyxl

EXPORT_SQL = """
SELECT *
FROM (
    SELECT
        'PeopleForce'::text AS source,
        'employee'::text AS entity,
        e.id AS source_id,
        e.full_name AS name,
        e.email AS email,
        e.personal_email AS personal_email,
        h.hr_master_id,
        h.pf_id AS master_pf_id,
        h.pf_full_name AS master_pf_full_name,
        h.email AS master_email,
        h.pipedrive_user_id AS master_pipedrive_user_id,
        h.pipedrive_person_id AS master_pipedrive_person_id
    FROM peopleforce_dm.employee e
    INNER JOIN master.hr_employee h ON h.pf_id = e.id

    UNION ALL

    SELECT
        'Pipedrive'::text,
        'user'::text,
        u.id,
        u.name,
        u.email,
        NULL::text,
        h.hr_master_id,
        h.pf_id,
        h.pf_full_name,
        h.email,
        h.pipedrive_user_id,
        h.pipedrive_person_id
    FROM pipedrive_dm.pipedrive_user u
    INNER JOIN master.hr_employee h
        ON lower(trim(h.email)) = lower(trim(u.email))
    WHERE COALESCE(trim(u.email), '') <> ''

    UNION ALL

    SELECT
        'Pipedrive'::text,
        'person'::text,
        p.id,
        COALESCE(NULLIF(trim(p.name), ''), trim(concat_ws(' ', p.first_name, p.last_name))),
        p.primary_email,
        NULL::text,
        h.hr_master_id,
        h.pf_id,
        h.pf_full_name,
        h.email,
        h.pipedrive_user_id,
        h.pipedrive_person_id
    FROM pipedrive_dm.person p
    INNER JOIN master.hr_employee h
        ON lower(trim(h.email)) = lower(trim(p.primary_email))
    WHERE COALESCE(trim(p.primary_email), '') <> ''
) t
ORDER BY source, entity, source_id
"""


def main() -> None:
    p = argparse.ArgumentParser(
        description="Excel: одна таблица найденных совпадений PF / Pipedrive ↔ master"
    )
    p.add_argument(
        "--output",
        "-o",
        type=Path,
        default=_ROOT / "files" / "hr_matched.xlsx",
        help="Путь к .xlsx",
    )
    args = p.parse_args()

    args.output.parent.mkdir(parents=True, exist_ok=True)

    from psycopg import errors as pg_errors

    wb = Workbook()
    ws = wb.active
    assert ws is not None
    ws.title = "matched"

    n = 0
    with connect(get_database_url()) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            try:
                cur.execute(EXPORT_SQL)
            except pg_errors.UndefinedTable as e:
                raise SystemExit(
                    "Нет таблиц master / peopleforce_dm / pipedrive_dm."
                ) from e
            desc = cur.description
            if desc:
                ws.append([d.name for d in desc])
                for row in cur:
                    ws.append([cell_value_for_openpyxl(x) for x in row])
                    n += 1

    wb.save(args.output)
    print(f"Строк: {n} → {args.output}", flush=True)


if __name__ == "__main__":
    main()

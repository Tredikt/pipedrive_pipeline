"""
Один лист Excel: сводка «сколько совпало» — привязки HR-мастера к PeopleForce и Pipedrive.

  python scripts/export_hr_matched_summary_xlsx.py
  python scripts/export_hr_matched_summary_xlsx.py -o files/hr_matched_summary.xlsx

Нужны master.hr_employee и витрины peopleforce_dm / pipedrive_dm.
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


SUMMARY_SQL = """
SELECT ord, metric, value
FROM (
    SELECT
        10 AS ord,
        'PeopleForce: всего сотрудников в витрине'::text AS metric,
        (SELECT COUNT(*)::bigint FROM peopleforce_dm.employee) AS value
    UNION ALL
    SELECT
        11,
        'PeopleForce: совпало — есть строка master с тем же pf_id',
        (
            SELECT COUNT(*)::bigint
            FROM peopleforce_dm.employee e
            WHERE EXISTS (
                SELECT 1 FROM master.hr_employee h WHERE h.pf_id = e.id
            )
        )
    UNION ALL
    SELECT
        12,
        'PeopleForce: не совпало — нет master.pf_id',
        (
            SELECT COUNT(*)::bigint
            FROM peopleforce_dm.employee e
            WHERE NOT EXISTS (
                SELECT 1 FROM master.hr_employee h WHERE h.pf_id = e.id
            )
        )
    UNION ALL
    SELECT
        20,
        'Pipedrive: пользователей с заполненным email в витрине',
        (
            SELECT COUNT(*)::bigint
            FROM pipedrive_dm.pipedrive_user u
            WHERE COALESCE(trim(u.email), '') <> ''
        )
    UNION ALL
    SELECT
        21,
        'Pipedrive: пользователей — совпало по email со строкой master',
        (
            SELECT COUNT(*)::bigint
            FROM pipedrive_dm.pipedrive_user u
            WHERE COALESCE(trim(u.email), '') <> ''
              AND EXISTS (
                  SELECT 1
                  FROM master.hr_employee h
                  WHERE lower(trim(h.email)) = lower(trim(u.email))
              )
        )
    UNION ALL
    SELECT
        22,
        'Pipedrive: пользователей — не совпало по email с master',
        (
            SELECT COUNT(*)::bigint
            FROM pipedrive_dm.pipedrive_user u
            WHERE COALESCE(trim(u.email), '') <> ''
              AND NOT EXISTS (
                  SELECT 1
                  FROM master.hr_employee h
                  WHERE lower(trim(h.email)) = lower(trim(u.email))
              )
        )
    UNION ALL
    SELECT
        30,
        'Pipedrive: контактов (person) с email в витрине',
        (
            SELECT COUNT(*)::bigint
            FROM pipedrive_dm.person p
            WHERE COALESCE(trim(p.primary_email), '') <> ''
        )
    UNION ALL
    SELECT
        31,
        'Pipedrive: контактов — совпало по email со строкой master',
        (
            SELECT COUNT(*)::bigint
            FROM pipedrive_dm.person p
            WHERE COALESCE(trim(p.primary_email), '') <> ''
              AND EXISTS (
                  SELECT 1
                  FROM master.hr_employee h
                  WHERE lower(trim(h.email)) = lower(trim(p.primary_email))
              )
        )
    UNION ALL
    SELECT
        32,
        'Pipedrive: контактов — не совпало по email с master',
        (
            SELECT COUNT(*)::bigint
            FROM pipedrive_dm.person p
            WHERE COALESCE(trim(p.primary_email), '') <> ''
              AND NOT EXISTS (
                  SELECT 1
                  FROM master.hr_employee h
                  WHERE lower(trim(h.email)) = lower(trim(p.primary_email))
              )
        )
    UNION ALL
    SELECT
        40,
        'HR master: всего строк',
        (SELECT COUNT(*)::bigint FROM master.hr_employee)
    UNION ALL
    SELECT
        41,
        'HR master: строк с заполненным pf_id',
        (
            SELECT COUNT(*)::bigint
            FROM master.hr_employee
            WHERE pf_id IS NOT NULL
        )
    UNION ALL
    SELECT
        42,
        'HR master: строк с pipedrive_user_id',
        (
            SELECT COUNT(*)::bigint
            FROM master.hr_employee
            WHERE pipedrive_user_id IS NOT NULL
        )
    UNION ALL
    SELECT
        43,
        'HR master: строк с pipedrive_person_id',
        (
            SELECT COUNT(*)::bigint
            FROM master.hr_employee
            WHERE pipedrive_person_id IS NOT NULL
        )
) t
ORDER BY ord
"""




def main() -> None:
    p = argparse.ArgumentParser(description="Excel: сводка совпадений HR master ↔ PF / PD")
    p.add_argument(
        "--output",
        "-o",
        type=Path,
        default=_ROOT / "files" / "hr_matched_summary.xlsx",
        help="Путь к .xlsx",
    )
    args = p.parse_args()

    args.output.parent.mkdir(parents=True, exist_ok=True)

    from psycopg import errors as pg_errors

    wb = Workbook()
    ws = wb.active
    assert ws is not None
    ws.title = "Совпало"

    n = 0
    with connect(get_database_url()) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            try:
                cur.execute(SUMMARY_SQL)
            except pg_errors.UndefinedTable as e:
                raise SystemExit(
                    "Нет таблиц master / peopleforce_dm / pipedrive_dm."
                ) from e
            desc = cur.description
            if desc:
                ws.append(["Показатель", "Значение"])
                for row in cur:
                    _, metric, value = row[0], row[1], row[2]
                    ws.append([metric, cell_value_for_openpyxl(value)])
                    n += 1

    wb.save(args.output)
    print(f"Строк сводки: {n} → {args.output}", flush=True)


if __name__ == "__main__":
    main()

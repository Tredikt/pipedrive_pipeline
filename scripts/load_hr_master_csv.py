"""
Загрузка CSV HR_Employees.csv в master.hr_employee (UPSERT по hr_master_id).

При необходимости создаёт схему/таблицу из sql/013_master_hr_employee.sql и
опционально колонки сверки / представления (016, 017).

  python scripts/load_hr_master_csv.py
  python scripts/load_hr_master_csv.py --csv files/HR_Employees.csv --init-schema

Нужен DATABASE_URL в .env.
"""
from __future__ import annotations

import argparse
import csv
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv

load_dotenv()

from src.config import get_database_url
from src.db import connect, init_schema


def _blank(v: Any) -> bool:
    if v is None:
        return True
    return isinstance(v, str) and not v.strip()


def _parse_bool(v: Any) -> bool | None:
    if _blank(v):
        return None
    s = str(v).strip().lower()
    if s in ("true", "1", "yes", "y"):
        return True
    if s in ("false", "0", "no", "n"):
        return False
    return None


def _parse_date(v: Any) -> date | None:
    if _blank(v):
        return None
    s = str(v).strip()[:10]
    try:
        y, m, d = (int(x) for x in s.split("-"))
        return date(y, m, d)
    except (ValueError, TypeError):
        return None


def _parse_ts(v: Any) -> datetime | None:
    if _blank(v):
        return None
    s = str(v).strip().replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return None


def _parse_bigint(v: Any) -> int | None:
    if _blank(v):
        return None
    try:
        return int(str(v).strip())
    except ValueError:
        return None


UPSERT_SQL = """
INSERT INTO master.hr_employee (
    hr_master_id,
    pf_id,
    pf_full_name,
    jira_id,
    jira_full_name,
    pipedrive_user_id,
    pipedrive_person_id,
    employment_company,
    employment_format,
    has_leaves,
    status,
    gender,
    email,
    mobile_number,
    date_of_birth,
    probation_ends_on,
    hired_on,
    position,
    job_level,
    location,
    employment_type,
    department,
    division,
    reporting_to,
    reporting_to_email,
    exception_note,
    source_created_at,
    source_updated_at
) VALUES (
    %(hr_master_id)s,
    %(pf_id)s,
    %(pf_full_name)s,
    %(jira_id)s,
    %(jira_full_name)s,
    %(pipedrive_user_id)s,
    %(pipedrive_person_id)s,
    %(employment_company)s,
    %(employment_format)s,
    %(has_leaves)s,
    %(status)s,
    %(gender)s,
    %(email)s,
    %(mobile_number)s,
    %(date_of_birth)s,
    %(probation_ends_on)s,
    %(hired_on)s,
    %(position)s,
    %(job_level)s,
    %(location)s,
    %(employment_type)s,
    %(department)s,
    %(division)s,
    %(reporting_to)s,
    %(reporting_to_email)s,
    %(exception_note)s,
    %(source_created_at)s,
    %(source_updated_at)s
)
ON CONFLICT (hr_master_id) DO UPDATE SET
    pf_id = EXCLUDED.pf_id,
    pf_full_name = EXCLUDED.pf_full_name,
    jira_id = EXCLUDED.jira_id,
    jira_full_name = EXCLUDED.jira_full_name,
    pipedrive_user_id = COALESCE(EXCLUDED.pipedrive_user_id, master.hr_employee.pipedrive_user_id),
    pipedrive_person_id = COALESCE(EXCLUDED.pipedrive_person_id, master.hr_employee.pipedrive_person_id),
    employment_company = EXCLUDED.employment_company,
    employment_format = EXCLUDED.employment_format,
    has_leaves = EXCLUDED.has_leaves,
    status = EXCLUDED.status,
    gender = EXCLUDED.gender,
    email = EXCLUDED.email,
    mobile_number = EXCLUDED.mobile_number,
    date_of_birth = EXCLUDED.date_of_birth,
    probation_ends_on = EXCLUDED.probation_ends_on,
    hired_on = EXCLUDED.hired_on,
    position = EXCLUDED.position,
    job_level = EXCLUDED.job_level,
    location = EXCLUDED.location,
    employment_type = EXCLUDED.employment_type,
    department = EXCLUDED.department,
    division = EXCLUDED.division,
    reporting_to = EXCLUDED.reporting_to,
    reporting_to_email = EXCLUDED.reporting_to_email,
    exception_note = EXCLUDED.exception_note,
    source_created_at = EXCLUDED.source_created_at,
    source_updated_at = EXCLUDED.source_updated_at,
    loaded_at = NOW();
"""


def row_to_record(row: dict[str, str]) -> dict[str, Any]:
    def g(key: str) -> str | None:
        v = row.get(key)
        if v is None:
            return None
        s = str(v).strip()
        return s if s else None

    return {
        "hr_master_id": _parse_bigint(row.get("id")),
        "pf_id": _parse_bigint(row.get("pf_id")),
        "pf_full_name": g("pf_full_name"),
        "jira_id": g("jira_id"),
        "jira_full_name": g("jira_full_name"),
        "pipedrive_user_id": None,
        "pipedrive_person_id": None,
        "employment_company": g("employment_company"),
        "employment_format": g("employment_format"),
        "has_leaves": _parse_bool(row.get("has_leaves")),
        "status": g("status"),
        "gender": g("gender"),
        "email": g("email"),
        "mobile_number": g("mobile_number"),
        "date_of_birth": _parse_date(row.get("date_of_birth")),
        "probation_ends_on": _parse_date(row.get("probation_ends_on")),
        "hired_on": _parse_date(row.get("hired_on")),
        "position": g("position"),
        "job_level": g("job_level"),
        "location": g("location"),
        "employment_type": g("employment_type"),
        "department": g("department"),
        "division": g("division"),
        "reporting_to": g("reporting_to"),
        "reporting_to_email": g("reporting_to_email"),
        "exception_note": g("exception"),
        "source_created_at": _parse_ts(row.get("createdAt")),
        "source_updated_at": _parse_ts(row.get("updatedAt")),
    }


def main() -> None:
    p = argparse.ArgumentParser(description="CSV → master.hr_employee")
    p.add_argument(
        "--csv",
        type=Path,
        default=_ROOT / "files" / "HR_Employees.csv",
        help="Путь к HR_Employees.csv",
    )
    p.add_argument(
        "--init-schema",
        action="store_true",
        help="Применить sql/013, 016, 017 перед загрузкой",
    )
    args = p.parse_args()

    if not args.csv.is_file():
        raise SystemExit(f"Файл не найден: {args.csv}")

    sql_paths = (
        _ROOT / "sql" / "013_master_hr_employee.sql",
        _ROOT / "sql" / "016_master_sync_columns.sql",
        _ROOT / "sql" / "017_master_views_reconciliation.sql",
    )
    for sql_path in sql_paths:
        if not sql_path.is_file():
            raise SystemExit(f"Нет SQL: {sql_path}")

    with connect(get_database_url()) as conn:
        if args.init_schema:
            for sql_path in sql_paths:
                init_schema(conn, str(sql_path))
                print(f"Схема применена: {sql_path}", flush=True)

        n = 0
        skipped = 0
        with conn.cursor() as cur:
            with args.csv.open(encoding="utf-8-sig", newline="") as fh:
                reader = csv.DictReader(fh)
                for row in reader:
                    rec = row_to_record(row)
                    hid = rec["hr_master_id"]
                    if hid is None:
                        skipped += 1
                        continue
                    cur.execute(UPSERT_SQL, rec)
                    n += 1
        conn.commit()

    print(f"Загружено строк: {n}; без id (пропуск): {skipped}", flush=True)


if __name__ == "__main__":
    main()

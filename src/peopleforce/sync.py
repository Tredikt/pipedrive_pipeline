"""Полная выгрузка PeopleForce API v3 -> peopleforce_dm (и опционально peopleforce_raw)."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Callable

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv

load_dotenv()

from src.config import get_database_url
from src.db import connect, init_schema
from src.peopleforce.bulk_endpoints import LEAVE_REQUEST_DEFAULT_PARAMS, entity_type_for_path
from src.peopleforce.bulk_raw import external_id_for_row, run_raw_bulk
from src.peopleforce.client import PeopleForceClient
from src.peopleforce.config import peopleforce_api_base_url, peopleforce_api_key
from src.peopleforce.dm_extra import PATH_DM_UPSERT
from src.peopleforce.parse import (
    flat_employee_row,
    merge_webhook_employee_data,
    upsert_department_row,
    upsert_division_row,
    upsert_employee_row,
    upsert_employment_type_row,
    upsert_entity_record,
    upsert_job_level_row,
    upsert_location_row,
    upsert_nested_refs_from_employee,
    upsert_position_row,
)

_SQL_009 = _ROOT / "sql" / "009_peopleforce.sql"
_SQL_010 = _ROOT / "sql" / "010_peopleforce_dm_extend.sql"
_SQL_011 = _ROOT / "sql" / "011_peopleforce_recruitment_candidate.sql"
_SQL_012 = _ROOT / "sql" / "012_peopleforce_dm_flat.sql"


def _init_peopleforce_schema(conn: Any) -> None:
    init_schema(conn, str(_SQL_009))
    init_schema(conn, str(_SQL_010))
    init_schema(conn, str(_SQL_011))
    init_schema(conn, str(_SQL_012))


def _ensure_dm_extend_schema(conn: Any) -> None:
    """
    После 009 без --init-db таблиц из 010 нет: один раз догоняем миграцию 010.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'peopleforce_dm' AND table_name = 'team'
            """
        )
        if cur.fetchone() is not None:
            return
    init_schema(conn, str(_SQL_010))
    print(
        "Прогнан sql/010_peopleforce_dm_extend.sql (доп. таблицы peopleforce_dm).",
        flush=True,
    )


def _ensure_recruitment_candidate_011(conn: Any) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'peopleforce_dm'
              AND table_name = 'recruitment_candidate'
              AND column_name = 'full_name'
            """
        )
        if cur.fetchone() is not None:
            return
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'peopleforce_dm'
              AND table_name = 'recruitment_candidate'
            """
        )
        if not cur.fetchone():
            return
    init_schema(conn, str(_SQL_011))
    print(
        "Прогнан sql/011_peopleforce_recruitment_candidate.sql (колонки кандидатов).",
        flush=True,
    )


def _ensure_dm_flat_012(conn: Any) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'peopleforce_dm'
              AND table_name = 'pay_schedule'
              AND column_name = 'created_at'
            """
        )
        if cur.fetchone() is not None:
            return
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'peopleforce_dm'
              AND table_name = 'pay_schedule'
            """
        )
        if not cur.fetchone():
            return
    init_schema(conn, str(_SQL_012))
    print(
        "Прогнан sql/012_peopleforce_dm_flat.sql (нормализованные поля витрины).",
        flush=True,
    )


def _sync_departments(
    client: PeopleForceClient, conn: Any, *, write_raw: bool
) -> int:
    n = 0
    with conn.cursor() as cur:
        for row in client.iter_paginated("/departments"):
            upsert_department_row(cur, row)
            if write_raw:
                upsert_entity_record(
                    cur,
                    entity_type="department",
                    external_id=str(row["id"]),
                    raw=row,
                )
            n += 1
    return n


def _sync_divisions(client: PeopleForceClient, conn: Any, *, write_raw: bool) -> int:
    n = 0
    with conn.cursor() as cur:
        for row in client.iter_paginated("/divisions"):
            upsert_division_row(cur, row)
            if write_raw:
                upsert_entity_record(
                    cur,
                    entity_type="division",
                    external_id=str(row["id"]),
                    raw=row,
                )
            n += 1
    return n


def _sync_locations(client: PeopleForceClient, conn: Any, *, write_raw: bool) -> int:
    n = 0
    with conn.cursor() as cur:
        for row in client.iter_paginated("/locations"):
            upsert_location_row(cur, row)
            if write_raw:
                upsert_entity_record(
                    cur,
                    entity_type="location",
                    external_id=str(row["id"]),
                    raw=row,
                )
            n += 1
    return n


def _sync_positions(client: PeopleForceClient, conn: Any, *, write_raw: bool) -> int:
    n = 0
    with conn.cursor() as cur:
        for row in client.iter_paginated("/positions", skip_on_client_error=True):
            upsert_position_row(cur, row)
            if write_raw and row.get("id") is not None:
                upsert_entity_record(
                    cur,
                    entity_type="position",
                    external_id=str(row["id"]),
                    raw=row,
                )
            n += 1
    return n


def _sync_job_levels(client: PeopleForceClient, conn: Any, *, write_raw: bool) -> int:
    n = 0
    with conn.cursor() as cur:
        for row in client.iter_paginated("/job_levels", skip_on_client_error=True):
            upsert_job_level_row(cur, row)
            if write_raw and row.get("id") is not None:
                upsert_entity_record(
                    cur,
                    entity_type="job_level",
                    external_id=str(row["id"]),
                    raw=row,
                )
            n += 1
    return n


def _sync_employment_types(
    client: PeopleForceClient, conn: Any, *, write_raw: bool
) -> int:
    n = 0
    with conn.cursor() as cur:
        for row in client.iter_paginated("/employment_types", skip_on_client_error=True):
            upsert_employment_type_row(cur, row)
            if write_raw and row.get("id") is not None:
                upsert_entity_record(
                    cur,
                    entity_type="employment_type",
                    external_id=str(row["id"]),
                    raw=row,
                )
            n += 1
    return n


def _sync_employees(
    client: PeopleForceClient, conn: Any, *, write_raw: bool, status: str = "all"
) -> int:
    n = 0
    with conn.cursor() as cur:
        for raw in client.iter_paginated(
            "/employees", extra_params={"status": status}
        ):
            r = merge_webhook_employee_data(raw) if "attributes" in raw else raw
            upsert_nested_refs_from_employee(cur, r)
            flat = flat_employee_row(r)
            upsert_employee_row(cur, flat)
            if write_raw:
                upsert_entity_record(
                    cur,
                    entity_type="employee",
                    external_id=str(flat["id"]),
                    raw=r,
                )
            n += 1
            if n % 200 == 0:
                conn.commit()
    return n


def _sync_path_dm(
    client: PeopleForceClient,
    conn: Any,
    path: str,
    extra: dict[str, Any] | None,
    *,
    write_raw: bool,
) -> int:
    fn: Callable[[Any, dict[str, Any]], bool] | None = PATH_DM_UPSERT.get(path)
    if fn is None:
        return 0
    n = 0
    with conn.cursor() as cur:
        for row in client.iter_paginated(
            path, extra_params=extra, skip_on_client_error=True
        ):
            if fn(cur, row):
                n += 1
            if write_raw:
                et = entity_type_for_path(path)
                upsert_entity_record(
                    cur,
                    entity_type=et,
                    external_id=external_id_for_row(row),
                    raw=row,
                )
    return n


# Порядок: department_levels вызывается отдельно до departments (см. run_sync).
DM_PATHS_AFTER_CORE: list[tuple[str, dict[str, Any] | None]] = [
    ("/teams", None),
    ("/leave_types", None),
    ("/public_holidays", None),
    ("/company_holidays", None),
    ("/leave_requests", dict(LEAVE_REQUEST_DEFAULT_PARAMS)),
    ("/work_schedules", None),
    ("/working_patterns", None),
    ("/time_entries", None),
    ("/shifts", None),
    ("/recruitment/vacancies", None),
    ("/recruitment/candidates", None),
    ("/recruitment/applications", None),
    ("/pay_schedules", None),
    ("/custom_fields/employees", None),
    ("/assets", None),
    ("/document_types", None),
    ("/cost_centers", None),
    ("/projects", None),
]


def run_sync(
    *,
    init_db: bool = False,
    schema_only: bool = False,
    only: str | None = None,
    all_raw: bool = False,
    write_raw: bool = False,
) -> None:
    if init_db or schema_only:
        with connect(get_database_url()) as conn:
            _init_peopleforce_schema(conn)
        print(
            "Схема peopleforce: 009 + 010 + 011 + 012 (применена).",
            flush=True,
        )
        if schema_only:
            return

    if all_raw:
        counts = run_raw_bulk()
        print(
            "peopleforce all-raw: "
            f"{sum(counts.values())} rows, {len(counts)} paths",
            flush=True,
        )
        return

    base = peopleforce_api_base_url()
    key = peopleforce_api_key()
    client = PeopleForceClient(base_url=base, api_key=key)

    with connect(get_database_url()) as conn:
        if not only:
            _ensure_dm_extend_schema(conn)
            _ensure_recruitment_candidate_011(conn)
            _ensure_dm_flat_012(conn)
        if only:
            n = 0
            if only == "employees":
                n = _sync_employees(client, conn, write_raw=write_raw, status="all")
            elif only == "departments":
                n = _sync_departments(client, conn, write_raw=write_raw)
            elif only == "divisions":
                n = _sync_divisions(client, conn, write_raw=write_raw)
            elif only == "locations":
                n = _sync_locations(client, conn, write_raw=write_raw)
            else:
                raise SystemExit(f"Unknown --only {only!r}")
            conn.commit()
            print(f"{only}: {n} rows", flush=True)
            return

        total = 0

        n_lv = _sync_path_dm(
            client, conn, "/department_levels", None, write_raw=write_raw
        )
        total += n_lv
        conn.commit()
        print(f"peopleforce department_levels: {n_lv} rows", flush=True)

        steps: list[tuple[str, Callable[[], int]]] = [
            ("departments", lambda: _sync_departments(client, conn, write_raw=write_raw)),
            ("divisions", lambda: _sync_divisions(client, conn, write_raw=write_raw)),
            ("locations", lambda: _sync_locations(client, conn, write_raw=write_raw)),
            ("positions", lambda: _sync_positions(client, conn, write_raw=write_raw)),
            ("job_levels", lambda: _sync_job_levels(client, conn, write_raw=write_raw)),
            (
                "employment_types",
                lambda: _sync_employment_types(client, conn, write_raw=write_raw),
            ),
        ]
        for label, fn in steps:
            n = fn()
            total += n
            conn.commit()
            print(f"peopleforce {label}: {n} rows", flush=True)

        n = _sync_employees(
            client, conn, write_raw=write_raw, status="all"
        )
        total += n
        conn.commit()
        print(f"peopleforce employees: {n} rows", flush=True)

        for path, extra in DM_PATHS_AFTER_CORE:
            if path not in PATH_DM_UPSERT:
                continue
            label = path.strip("/").replace("/", "_")
            n = _sync_path_dm(client, conn, path, extra, write_raw=write_raw)
            total += n
            conn.commit()
            print(f"peopleforce {label}: {n} rows", flush=True)

        print(f"peopleforce total rows (sum of steps): {total}", flush=True)


def main() -> None:
    p = argparse.ArgumentParser(
        description="Sync PeopleForce -> PostgreSQL (peopleforce_dm, опц. peopleforce_raw)"
    )
    p.add_argument(
        "--init-db",
        action="store_true",
        help="Apply sql/009..012 (схема PeopleForce + DM + нормализованные поля)",
    )
    p.add_argument(
        "--schema-only",
        action="store_true",
        help="Only apply SQL and exit (no API)",
    )
    p.add_argument(
        "--only",
        choices=("employees", "departments", "divisions", "locations"),
        default=None,
    )
    p.add_argument(
        "--all-raw",
        action="store_true",
        help="Сырая выгрузка (bulk_endpoints) только в peopleforce_raw.entity_record",
    )
    p.add_argument(
        "--raw",
        action="store_true",
        help="Дублировать в peopleforce_raw.entity_record при записи в DM",
    )
    args = p.parse_args()
    run_sync(
        init_db=args.init_db,
        schema_only=args.schema_only,
        only=args.only,
        all_raw=args.all_raw,
        write_raw=args.raw,
    )


if __name__ == "__main__":
    main()

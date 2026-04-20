"""
Аудит структуры по ТЗ: применить SQL, проверить наличие таблиц/представлений,
пробой API по каждой сущности, счётчики строк в БД.

Запуск из корня проекта:
  python scripts/tz_structure_audit.py
  python scripts/tz_structure_audit.py --no-schema   # не выполнять DDL
  python scripts/tz_structure_audit.py --no-api      # только БД

Нужны DATABASE_URL; для --no-api не нужен Pipedrive.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import httpx

from src.config import get_database_url, get_settings
from src.db import connect, init_schema
from src.entities import ENTITY_SPECS, EntitySpec
from src.pipedrive_client import PipedriveClient
from src.tz_structure import (
    ENTITY_DM_TABLE,
    REQUIRED_DM_VIEWS,
    REQUIRED_RAW_TABLES,
    SQL_MIGRATION_FILES,
)


def _apply_schema(conn) -> None:
    root = _ROOT / "sql"
    for name in SQL_MIGRATION_FILES:
        init_schema(conn, str(root / name))


def _table_exists(cur, schema: str, table: str) -> bool:
    cur.execute(
        """
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = %s AND table_name = %s AND table_type = 'BASE TABLE'
        """,
        (schema, table),
    )
    return cur.fetchone() is not None


def _view_exists(cur, schema: str, name: str) -> bool:
    cur.execute(
        """
        SELECT 1 FROM information_schema.views
        WHERE table_schema = %s AND table_name = %s
        """,
        (schema, name),
    )
    return cur.fetchone() is not None


def _count_rows(cur, schema: str, table: str) -> int | None:
    try:
        cur.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
        r = cur.fetchone()
        return int(r[0]) if r else None
    except Exception:
        return None


def _count_entity_record(cur, entity_type: str) -> int | None:
    try:
        cur.execute(
            "SELECT COUNT(*) FROM pipedrive_raw.entity_record WHERE entity_type = %s",
            (entity_type,),
        )
        r = cur.fetchone()
        return int(r[0]) if r else None
    except Exception:
        return None


def _probe_api(client: PipedriveClient, spec: EntitySpec) -> str:
    lim = max(1, spec.page_size or 1)
    try:
        body = client.get_json(spec.list_path, params={"start": 0, "limit": lim})
        if not body.get("success", True):
            return "API success=false"
        return "API OK"
    except httpx.HTTPStatusError as e:
        return f"API HTTP {e.response.status_code}"
    except Exception as e:
        return f"API err {type(e).__name__}: {e}"


def main() -> int:
    p = argparse.ArgumentParser(description="Аудит структуры БД и доступности API Pipedrive")
    p.add_argument("--no-schema", action="store_true", help="Не применять sql/*.sql")
    p.add_argument("--no-api", action="store_true", help="Не дергать Pipedrive")
    args = p.parse_args()

    db_url = get_database_url()
    client: PipedriveClient | None = None
    if not args.no_api:
        s = get_settings()
        client = PipedriveClient(base_url=s.pipedrive_api_base_url, api_token=s.pipedrive_api_token)

    print("=" * 100)
    print("1. DDL / объекты PostgreSQL")
    print("=" * 100)

    with connect(db_url) as conn:
        if not args.no_schema:
            _apply_schema(conn)
            conn.commit()
            print("Миграции применены:", ", ".join(SQL_MIGRATION_FILES))
        else:
            print("Пропуск DDL (--no-schema)")

        with conn.cursor() as cur:
            ddl_ok = True
            for sch, tbl in REQUIRED_RAW_TABLES:
                ex = _table_exists(cur, sch, tbl)
                print(f"  TABLE {sch}.{tbl}: {'да' if ex else 'НЕТ'}")
                ddl_ok = ddl_ok and ex
            for entity_name, dm_tbl in ENTITY_DM_TABLE.items():
                sch, tbl = "pipedrive_dm", dm_tbl
                ex = _table_exists(cur, sch, tbl)
                print(f"  TABLE {sch}.{tbl} ({entity_name}): {'да' if ex else 'НЕТ'}")
                ddl_ok = ddl_ok and ex
            for sch, v in REQUIRED_DM_VIEWS:
                ex = _view_exists(cur, sch, v)
                print(f"  VIEW {sch}.{v}: {'да' if ex else 'НЕТ'}")
                ddl_ok = ddl_ok and ex

    print()
    print("=" * 100)
    print("2. Сущности: API + строки в БД (entity_record или витрина)")
    print("=" * 100)
    print(f"{'entity':<28} {'api':<22} {'dm_table':<22} {'ddl':<6} {'rows':>10}")
    print("-" * 100)

    with connect(db_url) as conn:
        with conn.cursor() as cur:
            for spec in ENTITY_SPECS:
                api_s = "— (no-api)" if client is None else _probe_api(client, spec)
                if spec.name in ENTITY_DM_TABLE:
                    dm_tbl = ENTITY_DM_TABLE[spec.name]
                    exists = _table_exists(cur, "pipedrive_dm", dm_tbl)
                    n = _count_rows(cur, "pipedrive_dm", dm_tbl) if exists else None
                    ddl_m = "да" if exists else "нет"
                else:
                    dm_tbl = "—"
                    exists = _table_exists(cur, "pipedrive_raw", "entity_record")
                    n = _count_entity_record(cur, spec.name) if exists else None
                    ddl_m = "да" if exists else "нет"

                row_s = str(n) if n is not None else "?"

                print(f"{spec.name:<28} {api_s:<22} {dm_tbl:<22} {ddl_m:<6} {row_s:>10}")

    print("=" * 100)
    print(
        "Итог DDL (таблицы/представления из ТЗ):",
        "OK" if ddl_ok else "FAIL — проверьте миграции и права на БД",
    )
    if client is not None:
        print("API: колонка api — доступность эндпоинта (402/403/404 на платных модулях норма).")
        print("rows: данные появятся после python -m src.sync (сейчас только текущие счётчики в БД).")
    return 0 if ddl_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())

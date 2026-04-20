from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
from typing import Any, Generator

import psycopg
@contextmanager
def connect(database_url: str) -> Generator[psycopg.Connection, None, None]:
    with psycopg.connect(database_url) as conn:
        yield conn


def init_schema(conn: psycopg.Connection, sql_path: str) -> None:
    with open(sql_path, encoding="utf-8") as f:
        sql = f.read()
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def upsert_field_definition(
    conn: psycopg.Connection,
    *,
    entity_type: str,
    field_key: str,
    field_name: str,
    field_type: str | None,
    options: Any,
    raw: dict[str, Any],
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipedrive_raw.field_definition (
                entity_type, field_key, field_name, field_type, options, raw
            ) VALUES (%s, %s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (entity_type, field_key) DO UPDATE SET
                field_name = EXCLUDED.field_name,
                field_type = EXCLUDED.field_type,
                options = EXCLUDED.options,
                raw = EXCLUDED.raw,
                synced_at = NOW();
            """,
            (
                entity_type,
                field_key,
                field_name,
                field_type,
                psycopg.types.json.Jsonb(options) if options is not None else None,
                psycopg.types.json.Jsonb(raw),
            ),
        )


def upsert_entity_record(
    conn: psycopg.Connection,
    *,
    entity_type: str,
    pipedrive_id: str,
    raw: dict[str, Any],
    custom_resolved: dict[str, Any],
    pipedrive_updated: datetime | None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipedrive_raw.entity_record (
                entity_type, pipedrive_id, raw, custom_resolved, pipedrive_updated
            ) VALUES (%s, %s, %s::jsonb, %s::jsonb, %s)
            ON CONFLICT (entity_type, pipedrive_id) DO UPDATE SET
                raw = EXCLUDED.raw,
                custom_resolved = EXCLUDED.custom_resolved,
                pipedrive_updated = EXCLUDED.pipedrive_updated,
                synced_at = NOW();
            """,
            (
                entity_type,
                pipedrive_id,
                psycopg.types.json.Jsonb(raw),
                psycopg.types.json.Jsonb(custom_resolved),
                pipedrive_updated,
            ),
        )


def parse_pipedrive_ts(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value
    s = str(value).strip().replace("Z", "+00:00")
    try:
        if " " in s and "T" not in s:
            s = s.replace(" ", "T", 1)
        return datetime.fromisoformat(s)
    except ValueError:
        try:
            return datetime.strptime(str(value).strip(), "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None

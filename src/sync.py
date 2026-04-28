"""Pipedrive → pipedrive_raw / pipedrive_dm: сущности тянутся PipedriveClient.iter_collection
и pipedrive_list_next_start (start/limit, additional_data.pagination)."""

from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path
from typing import Any, Iterator

import httpx
from psycopg.rows import dict_row

from src.config import get_database_url, get_settings
from src.db import connect, init_schema, parse_pipedrive_ts, upsert_entity_record, upsert_field_definition
from src.dm_crm_entities import (
    upsert_activity_dm,
    upsert_call_log_dm,
    upsert_file_dm,
    upsert_lead_dm,
    upsert_note_dm,
    upsert_product_dm,
    upsert_project_dm,
    upsert_user_from_users_api,
)
from src.dm_reference import (
    upsert_currency_dm,
    upsert_deal_product_dm,
    upsert_pipeline_dm,
    upsert_stage_dm,
)
from src.dm_upsert import upsert_deal_dm, upsert_organization_dm, upsert_person_dm
from src.entities import ENTITY_SPECS, EntitySpec
from src.pipedrive_client import (
    PipedriveClient,
    PipedriveEndpointUnreadableError,
    pipedrive_list_next_start,
)
from src.webhook_parse import merge_api_row_with_webhook, row_from_webhook_body
from src.transform import (
    build_field_key_to_label,
    extract_custom_resolved,
    iter_custom_field_rows,
    standard_skip_keys,
)

logger = logging.getLogger(__name__)

PARENT_ENTITY_FOR_FIELD_SPEC: dict[str, str] = {
    "deal_fields": "deals",
    "person_fields": "persons",
    "organization_fields": "organizations",
    "product_fields": "products",
    "activity_fields": "activities",
    "lead_fields": "leads",
    "note_fields": "notes",
    "project_fields": "projects",
}

ENTITIES_WITH_CUSTOM_FIELDS: frozenset[str] = frozenset(PARENT_ENTITY_FOR_FIELD_SPEC.values())

# Нормализованные таблицы pipedrive_dm вместо raw JSON в entity_record
DM_ENTITIES: frozenset[str] = frozenset(
    {
        "persons",
        "organizations",
        "deals",
        "activities",
        "leads",
        "products",
        "notes",
        "call_logs",
        "files",
        "projects",
        "users",
        "pipelines",
        "stages",
        "currencies",
        "deal_products",
    }
)


def _load_field_rows(client: PipedriveClient, fields_path: str) -> list[dict[str, Any]]:
    """Все определения полей (*Fields), с пагинацией start/limit (как у списков сущностей)."""
    out: list[dict[str, Any]] = []
    start = 0
    limit = 500
    while True:
        body = client.get_json(fields_path, params={"limit": limit, "start": start})
        if not body.get("success", True):
            raise RuntimeError(f"Pipedrive fields error {fields_path}: {body}")
        data = body.get("data")
        if data is None:
            break
        if isinstance(data, list):
            batch = [x for x in data if isinstance(x, dict)]
        elif isinstance(data, dict):
            batch = [v for v in data.values() if isinstance(v, dict)]
        else:
            batch = []
        out.extend(batch)
        nxt = pipedrive_list_next_start(
            body, start=start, page_size=limit, row_count=len(batch)
        )
        if nxt is None:
            break
        start = nxt
    return out


def _sync_field_definitions_for_entity(
    client: PipedriveClient,
    conn: Any,
    *,
    logical_entity: str,
    fields_path: str,
) -> dict[str, str]:
    rows = _load_field_rows(client, fields_path)
    for row in rows:
        key = row.get("key")
        if not key:
            continue
        upsert_field_definition(
            conn,
            entity_type=logical_entity,
            field_key=str(key),
            field_name=str(row.get("name") or key),
            field_type=row.get("field_type") if isinstance(row.get("field_type"), str) else None,
            options=row.get("options"),
            raw=row,
        )
    conn.commit()
    return build_field_key_to_label(rows)


def _field_mapping_from_db(conn: Any, logical_entity: str) -> dict[str, str]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT field_key, field_name FROM pipedrive_raw.field_definition
            WHERE entity_type = %s
            """,
            (logical_entity,),
        )
        db_rows = [{"key": r["field_key"], "name": r["field_name"]} for r in cur.fetchall()]
    return build_field_key_to_label(db_rows)


def _record_id(row: dict[str, Any], id_key: str) -> str:
    """
    Стабильный ключ строки для pipedrive_raw / DM.

    У части справочников (например lead_sources) бывают встроенные записи только с name,
    без числового id — тогда используем key/type или синтетический id по имени / хэшу.
    """
    v = row.get(id_key)
    if v is not None and str(v).strip() != "":
        return str(v).strip()
    for alt in ("key", "type", "uuid"):
        a = row.get(alt)
        if a is not None and str(a).strip() != "":
            return str(a).strip()
    name = row.get("name")
    if isinstance(name, str) and name.strip():
        return f"name:{name.strip()}"
    h = hashlib.sha256(
        json.dumps(row, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()[:40]
    return f"sha256:{h}"


def _iter_entity_rows(
    client: PipedriveClient,
    spec: EntitySpec,
) -> Iterator[dict[str, Any]]:
    """Пагинация; 400/402/403/404/405 — пропуск (эндпоинт недоступен / иначе в API), без падения синка."""
    page = spec.page_size if spec.page_size is not None else 500
    try:
        yield from client.iter_collection(spec.list_path, page_size=page)
    except httpx.HTTPStatusError as e:
        code = e.response.status_code
        if code in (400, 402, 403, 404, 405):
            print(f"{spec.name}: пропущено (HTTP {code}) {spec.list_path}")
            return
        raise
    except PipedriveEndpointUnreadableError as e:
        print(f"{spec.name}: пропущено (не JSON / пустой ответ): {spec.list_path} ({e})")
        return


def resolve_key_to_label_webhook(conn: Any, spec: EntitySpec) -> dict[str, str]:
    """Для webhooks: маппинг полей только из БД (без повторной загрузки *Fields)."""
    if spec.name in ENTITIES_WITH_CUSTOM_FIELDS:
        return _field_mapping_from_db(conn, spec.name)
    return {}


def store_entity_row(
    conn: Any,
    spec: EntitySpec,
    raw: dict[str, Any],
    *,
    key_to_label: dict[str, str],
    parent: str | None,
) -> None:
    """Одна строка сущности → DM + entity_record (как в sync_entity_spec)."""
    pid = _record_id(raw, spec.id_key)
    if parent:
        fk = raw.get("key")
        if fk:
            upsert_field_definition(
                conn,
                entity_type=parent,
                field_key=str(fk),
                field_name=str(raw.get("name") or fk),
                field_type=raw.get("field_type") if isinstance(raw.get("field_type"), str) else None,
                options=raw.get("options"),
                raw=raw,
            )
        custom: dict[str, Any] = {}
    elif spec.fields_path:
        custom = extract_custom_resolved(raw, key_to_label, skip_keys=standard_skip_keys(spec.name))
    else:
        custom = {}

    custom_rows = iter_custom_field_rows(raw, key_to_label, skip_keys=standard_skip_keys(spec.name))

    if spec.name == "persons":
        upsert_person_dm(conn, raw, custom_rows=custom_rows)
    elif spec.name == "organizations":
        upsert_organization_dm(conn, raw, custom_rows=custom_rows)
    elif spec.name == "deals":
        upsert_deal_dm(conn, raw, custom_rows=custom_rows)
    elif spec.name == "activities":
        upsert_activity_dm(conn, raw, custom_rows=custom_rows)
    elif spec.name == "leads":
        upsert_lead_dm(conn, raw, custom_rows=custom_rows)
    elif spec.name == "products":
        upsert_product_dm(conn, raw, custom_rows=custom_rows)
    elif spec.name == "notes":
        upsert_note_dm(conn, raw, custom_rows=custom_rows)
    elif spec.name == "call_logs":
        upsert_call_log_dm(conn, raw, custom_rows=custom_rows)
    elif spec.name == "files":
        upsert_file_dm(conn, raw)
    elif spec.name == "projects":
        upsert_project_dm(conn, raw, custom_rows=custom_rows)
    elif spec.name == "users":
        upsert_user_from_users_api(conn, raw)
    elif spec.name == "pipelines":
        upsert_pipeline_dm(conn, raw)
    elif spec.name == "stages":
        upsert_stage_dm(conn, raw)
    elif spec.name == "currencies":
        upsert_currency_dm(conn, raw)
    elif spec.name == "deal_products":
        upsert_deal_product_dm(conn, raw)
    elif spec.name not in DM_ENTITIES:
        updated = parse_pipedrive_ts(raw.get("update_time") or raw.get("updateTime"))
        upsert_entity_record(
            conn,
            entity_type=spec.name,
            pipedrive_id=pid,
            raw=raw,
            custom_resolved=custom,
            pipedrive_updated=updated,
        )

    if spec.name in DM_ENTITIES:
        updated_dm = parse_pipedrive_ts(raw.get("update_time") or raw.get("updateTime"))
        upsert_entity_record(
            conn,
            entity_type=spec.name,
            pipedrive_id=pid,
            raw=raw,
            custom_resolved=custom,
            pipedrive_updated=updated_dm,
        )


def sync_entity_spec(client: PipedriveClient, conn: Any, spec: EntitySpec) -> int:
    parent = PARENT_ENTITY_FOR_FIELD_SPEC.get(spec.name)
    key_to_label: dict[str, str] = {}

    print(f"{spec.name}: старт (поля + список из API)…", flush=True)

    if spec.fields_path and not parent:
        try:
            key_to_label = _sync_field_definitions_for_entity(
                client, conn, logical_entity=spec.name, fields_path=spec.fields_path
            )
        except httpx.HTTPStatusError as e:
            if e.response.status_code in (402, 403, 404):
                print(
                    f"{spec.name}: метаданные полей пропущены (HTTP {e.response.status_code}) "
                    f"{spec.fields_path}"
                )
                key_to_label = {}
            else:
                raise
    if not parent and spec.name in ENTITIES_WITH_CUSTOM_FIELDS and not key_to_label:
        key_to_label = _field_mapping_from_db(conn, spec.name)

    if spec.fields_path and not parent:
        print(
            f"{spec.name}: поля загружены ({len(key_to_label)} в маппинге), идёт список {spec.list_path}…",
            flush=True,
        )
    else:
        print(f"{spec.name}: список {spec.list_path}…", flush=True)

    count = 0

    for row in _iter_entity_rows(client, spec):
        raw = dict(row)
        store_entity_row(conn, spec, raw, key_to_label=key_to_label, parent=parent)
        count += 1
        if count % 200 == 0:
            conn.commit()
        if count % 500 == 0:
            print(f"  {spec.name}: {count} строк…", flush=True)

    conn.commit()
    return count


def sync_one_entity_by_id(
    client: PipedriveClient,
    conn: Any,
    spec_name: str,
    entity_id: str,
) -> bool:
    """GET /v1/{entity}/{id} и запись в БД (без тела webhook)."""
    return sync_one_entity_webhook(client, conn, spec_name, entity_id, webhook_body=None)


def sync_one_entity_webhook(
    client: PipedriveClient,
    conn: Any,
    spec_name: str,
    entity_id: str,
    webhook_body: dict[str, Any] | None,
) -> bool:
    """
    Одна сущность в БД: строка из GET объединяется с data из webhook v2;
    webhook без-null полей перекрывает GET (лиды/files при частичном payload или 404 на GET).
    """
    spec = next((s for s in ENTITY_SPECS if s.name == spec_name), None)
    if spec is None:
        return False
    parent = PARENT_ENTITY_FOR_FIELD_SPEC.get(spec.name)
    if parent:
        return False
    key_to_label = resolve_key_to_label_webhook(conn, spec)

    row_wh: dict[str, Any] | None = None
    if webhook_body:
        row_wh = row_from_webhook_body(webhook_body, entity_id)
        if row_wh is not None:
            logger.info(
                "Webhook data row spec=%s id=%s — merge с GET при наличии",
                spec_name,
                entity_id,
            )
    row_api = client.get_item(spec.list_path, entity_id)
    row = merge_api_row_with_webhook(row_api, row_wh)
    if row is None:
        logger.warning(
            "Нет данных для upsert: spec=%s id=%s (GET и webhook пусты или несовместимы)",
            spec_name,
            entity_id,
        )
        return False
    store_entity_row(conn, spec, row, key_to_label=key_to_label, parent=parent)
    return True


def run_sync(
    *,
    init_db: bool = False,
    only: str | None = None,
    schema_only: bool = False,
    skip: frozenset[str] | None = None,
) -> None:
    root = Path(__file__).resolve().parents[1]
    sql_file = root / "sql" / "001_init_schema.sql"

    if init_db or schema_only:
        with connect(get_database_url()) as conn:
            init_schema(conn, str(sql_file))
            init_schema(conn, str(root / "sql" / "002_pipedrive_dm.sql"))
            init_schema(conn, str(root / "sql" / "003_dm_crm_core_entities.sql"))
            init_schema(conn, str(root / "sql" / "004_dm_custom_fields_labeled_view.sql"))
            init_schema(conn, str(root / "sql" / "006_view_phone_calls_from_activities.sql"))
            init_schema(conn, str(root / "sql" / "007_dm_reference_and_deal_products.sql"))
            init_schema(conn, str(root / "sql" / "008_v_custom_fields_pipedrive_name.sql"))
        print("Схемы pipedrive_raw и pipedrive_dm применены.")
        if schema_only:
            return

    settings = get_settings()
    client = PipedriveClient(
        base_url=settings.pipedrive_api_base_url,
        api_token=settings.pipedrive_api_token,
    )

    with connect(settings.database_url) as conn:
        print("PostgreSQL: подключено. Старт выгрузки сущностей…", flush=True)
        specs = ENTITY_SPECS
        if only:
            specs = tuple(s for s in ENTITY_SPECS if s.name == only)
            if not specs:
                raise RuntimeError(f"Unknown entity {only!r}")
        sk = skip or frozenset()
        if sk:
            unknown = sk - {s.name for s in ENTITY_SPECS}
            if unknown:
                raise RuntimeError(
                    f"Неизвестные имена в --skip: {sorted(unknown)}. "
                    f"Допустимы: {', '.join(sorted(s.name for s in ENTITY_SPECS))}"
                )
            specs = tuple(s for s in specs if s.name not in sk)
            print(f"Пропуск (--skip): {', '.join(sorted(sk))}", flush=True)

        if not specs:
            print("Нечего синхронизировать (пустой список после --only/--skip).", flush=True)
            return

        for spec in specs:
            n = sync_entity_spec(client, conn, spec)
            print(f"{spec.name}: {n} rows", flush=True)


def __main__() -> None:
    import argparse

    p = argparse.ArgumentParser(description="Sync Pipedrive -> PostgreSQL (pipedrive_raw)")
    p.add_argument("--init-db", action="store_true", help="Apply sql/001_init_schema.sql")
    p.add_argument(
        "--schema-only",
        action="store_true",
        help="Только применить SQL-схему и выйти (без Pipedrive)",
    )
    p.add_argument("--only", default=None, help="Sync single entity name, e.g. deals")
    p.add_argument(
        "--skip",
        default="",
        metavar="NAMES",
        help="Не синхронизировать (имена через запятую), например: deals,persons,files",
    )
    args = p.parse_args()
    raw_skip = (args.skip or "").strip()
    skip_set: frozenset[str] | None = None
    if raw_skip:
        parts = [x.strip() for x in raw_skip.replace(";", ",").split(",")]
        skip_set = frozenset(p for p in parts if p)
    run_sync(
        init_db=args.init_db,
        only=args.only,
        schema_only=args.schema_only,
        skip=skip_set,
    )


if __name__ == "__main__":
    __main__()

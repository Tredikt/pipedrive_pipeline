"""Витрина pipedrive_dm для activities, leads, products, notes, call_logs, files, projects, users."""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any

import psycopg

from src.db import parse_pipedrive_ts
from src.dm_upsert import _ref_id, _safe_int, _to_date, replace_custom_fields, upsert_pipedrive_user


def _as_bool(val: Any) -> bool | None:
    if val is None:
        return None
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)):
        return bool(val)
    if isinstance(val, str) and val.lower() in ("0", "false", ""):
        return False
    if isinstance(val, str) and val.lower() in ("1", "true"):
        return True
    return None


def _as_text_column(val: Any) -> str | None:
    """Webhooks v2 отдают вложенные dict/list в полях, где GET API — плоская строка."""
    if val is None:
        return None
    if isinstance(val, str):
        return val
    if isinstance(val, (int, float, bool, Decimal)):
        return str(val)
    if isinstance(val, (dict, list)):
        return json.dumps(val, ensure_ascii=False, sort_keys=True)
    return str(val)


def _activity_type_str(val: Any) -> str | None:
    if val is None:
        return None
    if isinstance(val, str):
        return val
    if isinstance(val, dict):
        k = val.get("key") or val.get("name") or val.get("type")
        if k is not None:
            return str(k)
        return json.dumps(val, ensure_ascii=False, sort_keys=True)
    return str(val)


def _int_field(val: Any) -> int | None:
    """ID из числа, строки или объекта { value, id } как в v2."""
    r = _ref_id(val)
    if r is not None:
        return r
    return _safe_int(val)


def _entity_id_str(row: dict[str, Any], id_key: str = "id") -> str | None:
    v = row.get(id_key)
    if v is None:
        return None
    return str(v).strip() or None


def upsert_user_from_users_api(conn: psycopg.Connection, row: dict[str, Any]) -> None:
    """Полная строка из GET /v1/users."""
    uid = _safe_int(row.get("id"))
    if uid is None:
        return
    has_pic = row.get("has_pic")
    if isinstance(has_pic, bool):
        has_pic = 1 if has_pic else 0
    active_f = _as_bool(row.get("active_flag"))
    activated = _as_bool(row.get("activated"))
    is_admin = _as_bool(row.get("is_admin"))
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipedrive_dm.pipedrive_user (
                id, name, email, active_flag, has_pic, pic_hash,
                lang, locale, timezone_name, phone, activated, last_login,
                created, modified, role_id, default_currency, icon_url, is_admin, timezone_offset
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                email = EXCLUDED.email,
                active_flag = EXCLUDED.active_flag,
                has_pic = EXCLUDED.has_pic,
                pic_hash = EXCLUDED.pic_hash,
                lang = EXCLUDED.lang,
                locale = EXCLUDED.locale,
                timezone_name = EXCLUDED.timezone_name,
                phone = EXCLUDED.phone,
                activated = EXCLUDED.activated,
                last_login = EXCLUDED.last_login,
                created = EXCLUDED.created,
                modified = EXCLUDED.modified,
                role_id = EXCLUDED.role_id,
                default_currency = EXCLUDED.default_currency,
                icon_url = EXCLUDED.icon_url,
                is_admin = EXCLUDED.is_admin,
                timezone_offset = EXCLUDED.timezone_offset,
                synced_at = NOW();
            """,
            (
                uid,
                row.get("name"),
                row.get("email"),
                active_f,
                has_pic,
                str(row["pic_hash"]) if row.get("pic_hash") else None,
                row.get("lang"),
                row.get("locale"),
                row.get("timezone_name"),
                row.get("phone"),
                activated,
                parse_pipedrive_ts(row.get("last_login")),
                parse_pipedrive_ts(row.get("created")),
                parse_pipedrive_ts(row.get("modified")),
                _safe_int(row.get("role_id")),
                row.get("default_currency"),
                row.get("icon_url"),
                is_admin,
                str(row["timezone_offset"]) if row.get("timezone_offset") is not None else None,
            ),
        )


def upsert_activity_dm(
    conn: psycopg.Connection,
    row: dict[str, Any],
    *,
    custom_rows: list[tuple[str, str, Any]],
) -> None:
    aid = _safe_int(row.get("id"))
    if aid is None:
        return
    owner_id = upsert_pipedrive_user(conn, row.get("owner_id"))
    uid = _int_field(row.get("user_id"))
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipedrive_dm.activity (
                id, subject, type, deal_id, person_id, org_id, owner_user_id, user_id,
                group_id, company_id, due_date, due_time, duration, note, busy_flag,
                public_description, done, location, add_time, update_time, marked_as_done_time
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (id) DO UPDATE SET
                subject = EXCLUDED.subject, type = EXCLUDED.type,
                deal_id = EXCLUDED.deal_id, person_id = EXCLUDED.person_id, org_id = EXCLUDED.org_id,
                owner_user_id = EXCLUDED.owner_user_id, user_id = EXCLUDED.user_id,
                group_id = EXCLUDED.group_id, company_id = EXCLUDED.company_id,
                due_date = EXCLUDED.due_date, due_time = EXCLUDED.due_time, duration = EXCLUDED.duration,
                note = EXCLUDED.note, busy_flag = EXCLUDED.busy_flag,
                public_description = EXCLUDED.public_description, done = EXCLUDED.done,
                location = EXCLUDED.location, add_time = EXCLUDED.add_time, update_time = EXCLUDED.update_time,
                marked_as_done_time = EXCLUDED.marked_as_done_time, synced_at = NOW();
            """,
            (
                aid,
                _as_text_column(row.get("subject")),
                _activity_type_str(row.get("type")),
                _ref_id(row.get("deal_id")),
                _ref_id(row.get("person_id")),
                _ref_id(row.get("org_id")),
                owner_id,
                uid,
                _int_field(row.get("group_id")),
                _int_field(row.get("company_id")),
                _to_date(row.get("due_date")),
                _as_text_column(row.get("due_time")),
                _as_text_column(row.get("duration")),
                _as_text_column(row.get("note")),
                _as_bool(row.get("busy_flag")),
                _as_text_column(row.get("public_description")),
                _as_bool(row.get("done")),
                _as_text_column(row.get("location")),
                parse_pipedrive_ts(row.get("add_time")),
                parse_pipedrive_ts(row.get("update_time")),
                parse_pipedrive_ts(row.get("marked_as_done_time")),
            ),
        )
    replace_custom_fields(conn, entity_type="activity", entity_id=str(aid), rows=custom_rows)


def upsert_lead_dm(
    conn: psycopg.Connection,
    row: dict[str, Any],
    *,
    custom_rows: list[tuple[str, str, Any]],
) -> None:
    lid = _entity_id_str(row, "id")
    if not lid:
        return
    owner_id = upsert_pipedrive_user(conn, row.get("owner_id"))
    val = row.get("value")
    if val is not None and not isinstance(val, Decimal):
        try:
            val = Decimal(str(val))
        except Exception:
            val = None
    label_ids = row.get("label_ids")
    if label_ids is not None and not isinstance(label_ids, list):
        label_ids = None
    elif label_ids:
        label_ids = [_safe_int(x) for x in label_ids if x is not None]
        label_ids = [x for x in label_ids if x is not None]
    org_id = _ref_id(row.get("organization_id") or row.get("org_id"))
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipedrive_dm.lead (
                id, title, owner_user_id, person_id, organization_id, pipeline_id, stage_id,
                value, currency, expected_close_date, source_name, archived, was_seen,
                label_ids, add_time, update_time
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                title = EXCLUDED.title, owner_user_id = EXCLUDED.owner_user_id,
                person_id = EXCLUDED.person_id, organization_id = EXCLUDED.organization_id,
                pipeline_id = EXCLUDED.pipeline_id, stage_id = EXCLUDED.stage_id,
                value = EXCLUDED.value, currency = EXCLUDED.currency,
                expected_close_date = EXCLUDED.expected_close_date, source_name = EXCLUDED.source_name,
                archived = EXCLUDED.archived, was_seen = EXCLUDED.was_seen,
                label_ids = EXCLUDED.label_ids, add_time = EXCLUDED.add_time,
                update_time = EXCLUDED.update_time, synced_at = NOW();
            """,
            (
                lid,
                row.get("title"),
                owner_id,
                _ref_id(row.get("person_id")),
                org_id,
                _safe_int(row.get("pipeline_id")),
                _safe_int(row.get("stage_id")),
                val,
                row.get("currency"),
                _to_date(row.get("expected_close_date")),
                row.get("source_name"),
                row.get("archived"),
                row.get("was_seen"),
                label_ids,
                parse_pipedrive_ts(row.get("add_time")),
                parse_pipedrive_ts(row.get("update_time")),
            ),
        )
    replace_custom_fields(conn, entity_type="lead", entity_id=lid, rows=custom_rows)


def upsert_product_dm(
    conn: psycopg.Connection,
    row: dict[str, Any],
    *,
    custom_rows: list[tuple[str, str, Any]],
) -> None:
    pid = _safe_int(row.get("id"))
    if pid is None:
        return
    owner_id = upsert_pipedrive_user(conn, row.get("owner_id"))
    tax = row.get("tax")
    if tax is not None:
        try:
            tax = Decimal(str(tax))
        except Exception:
            tax = None
    prices = row.get("prices")
    pj = psycopg.types.json.Jsonb(prices) if isinstance(prices, (dict, list)) else None
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipedrive_dm.product (
                id, name, code, unit, tax, category, owner_user_id, active_flag,
                selectable, first_char, prices, add_time, update_time
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name, code = EXCLUDED.code, unit = EXCLUDED.unit, tax = EXCLUDED.tax,
                category = EXCLUDED.category, owner_user_id = EXCLUDED.owner_user_id,
                active_flag = EXCLUDED.active_flag, selectable = EXCLUDED.selectable,
                first_char = EXCLUDED.first_char, prices = EXCLUDED.prices,
                add_time = EXCLUDED.add_time, update_time = EXCLUDED.update_time, synced_at = NOW();
            """,
            (
                pid,
                row.get("name"),
                row.get("code"),
                row.get("unit"),
                tax,
                row.get("category"),
                owner_id,
                row.get("active_flag"),
                row.get("selectable"),
                (str(row.get("first_char") or "")[:1] or None),
                pj,
                parse_pipedrive_ts(row.get("add_time")),
                parse_pipedrive_ts(row.get("update_time")),
            ),
        )
    replace_custom_fields(conn, entity_type="product", entity_id=str(pid), rows=custom_rows)


def upsert_note_dm(
    conn: psycopg.Connection,
    row: dict[str, Any],
    *,
    custom_rows: list[tuple[str, str, Any]],
) -> None:
    nid = _safe_int(row.get("id"))
    if nid is None:
        return
    add_u = upsert_pipedrive_user(conn, row.get("add_user_id") or row.get("user_id"))
    lead_raw = row.get("lead_id")
    lead_id = str(lead_raw).strip() if lead_raw is not None and lead_raw != "" else None
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipedrive_dm.note (
                id, content, deal_id, person_id, org_id, lead_id, project_id,
                add_user_id, update_user_id, pinned_to_deal_flag,
                pinned_to_organization_flag, pinned_to_person_flag, add_time, update_time
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                content = EXCLUDED.content, deal_id = EXCLUDED.deal_id,
                person_id = EXCLUDED.person_id, org_id = EXCLUDED.org_id, lead_id = EXCLUDED.lead_id,
                project_id = EXCLUDED.project_id, add_user_id = EXCLUDED.add_user_id,
                update_user_id = EXCLUDED.update_user_id, pinned_to_deal_flag = EXCLUDED.pinned_to_deal_flag,
                pinned_to_organization_flag = EXCLUDED.pinned_to_organization_flag,
                pinned_to_person_flag = EXCLUDED.pinned_to_person_flag,
                add_time = EXCLUDED.add_time, update_time = EXCLUDED.update_time, synced_at = NOW();
            """,
            (
                nid,
                row.get("content"),
                _ref_id(row.get("deal_id")),
                _ref_id(row.get("person_id")),
                _ref_id(row.get("org_id")),
                lead_id,
                _safe_int(row.get("project_id")),
                add_u,
                _safe_int(row.get("update_user_id")),
                row.get("pinned_to_deal_flag"),
                row.get("pinned_to_organization_flag"),
                row.get("pinned_to_person_flag"),
                parse_pipedrive_ts(row.get("add_time")),
                parse_pipedrive_ts(row.get("update_time")),
            ),
        )
    replace_custom_fields(conn, entity_type="note", entity_id=str(nid), rows=custom_rows)


def upsert_call_log_dm(
    conn: psycopg.Connection,
    row: dict[str, Any],
    *,
    custom_rows: list[tuple[str, str, Any]],
) -> None:
    cid = _entity_id_str(row, "id")
    if not cid:
        return
    user_id = upsert_pipedrive_user(conn, row.get("user_id"))
    lead_raw = row.get("lead_id")
    lead_id = str(lead_raw).strip() if lead_raw is not None and lead_raw != "" else None
    dur = row.get("duration")
    if dur is not None:
        try:
            dur = Decimal(str(dur))
        except Exception:
            dur = None
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipedrive_dm.call_log (
                id, activity_id, deal_id, person_id, org_id, lead_id, user_id,
                subject, duration, outcome, from_phone_number, to_phone_number,
                start_time, end_time, note, company_id, add_time, update_time
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                activity_id = EXCLUDED.activity_id, deal_id = EXCLUDED.deal_id,
                person_id = EXCLUDED.person_id, org_id = EXCLUDED.org_id, lead_id = EXCLUDED.lead_id,
                user_id = EXCLUDED.user_id, subject = EXCLUDED.subject, duration = EXCLUDED.duration,
                outcome = EXCLUDED.outcome, from_phone_number = EXCLUDED.from_phone_number,
                to_phone_number = EXCLUDED.to_phone_number, start_time = EXCLUDED.start_time,
                end_time = EXCLUDED.end_time, note = EXCLUDED.note, company_id = EXCLUDED.company_id,
                add_time = EXCLUDED.add_time, update_time = EXCLUDED.update_time, synced_at = NOW();
            """,
            (
                cid,
                _safe_int(row.get("activity_id")),
                _ref_id(row.get("deal_id")),
                _ref_id(row.get("person_id")),
                _ref_id(row.get("org_id")),
                lead_id,
                user_id,
                row.get("subject"),
                dur,
                str(row["outcome"]) if row.get("outcome") is not None else None,
                row.get("from_phone_number"),
                row.get("to_phone_number"),
                parse_pipedrive_ts(row.get("start_time")),
                parse_pipedrive_ts(row.get("end_time")),
                row.get("note"),
                _safe_int(row.get("company_id")),
                parse_pipedrive_ts(row.get("add_time")),
                parse_pipedrive_ts(row.get("update_time")),
            ),
        )
    replace_custom_fields(conn, entity_type="call_log", entity_id=cid, rows=custom_rows)


def upsert_file_dm(conn: psycopg.Connection, row: dict[str, Any]) -> None:
    fid = _safe_int(row.get("id"))
    if fid is None:
        return
    add_u = upsert_pipedrive_user(conn, row.get("user_id") or row.get("add_user_id"))
    lead_raw = row.get("lead_id")
    lead_id = str(lead_raw).strip() if lead_raw is not None and lead_raw != "" else None
    log_raw = row.get("log_id")
    log_id = str(log_raw) if log_raw is not None and log_raw != "" else None
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipedrive_dm.file (
                id, name, file_type, file_size, remote_location, s3_bucket, url,
                deal_id, person_id, org_id, product_id, lead_id, activity_id, project_id,
                mail_message_id, log_id, add_user_id, cid, add_time, update_time
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name, file_type = EXCLUDED.file_type, file_size = EXCLUDED.file_size,
                remote_location = EXCLUDED.remote_location, s3_bucket = EXCLUDED.s3_bucket, url = EXCLUDED.url,
                deal_id = EXCLUDED.deal_id, person_id = EXCLUDED.person_id, org_id = EXCLUDED.org_id,
                product_id = EXCLUDED.product_id, lead_id = EXCLUDED.lead_id,
                activity_id = EXCLUDED.activity_id, project_id = EXCLUDED.project_id,
                mail_message_id = EXCLUDED.mail_message_id, log_id = EXCLUDED.log_id,
                add_user_id = EXCLUDED.add_user_id, cid = EXCLUDED.cid,
                add_time = EXCLUDED.add_time, update_time = EXCLUDED.update_time, synced_at = NOW();
            """,
            (
                fid,
                row.get("name"),
                row.get("file_type"),
                _safe_int(row.get("file_size")),
                row.get("remote_location") or row.get("file_remote_location"),
                row.get("s3_bucket"),
                row.get("url"),
                _ref_id(row.get("deal_id")),
                _ref_id(row.get("person_id")),
                _ref_id(row.get("org_id")),
                _ref_id(row.get("product_id")),
                lead_id,
                _safe_int(row.get("activity_id")),
                _safe_int(row.get("project_id")),
                _safe_int(row.get("mail_message_id")),
                log_id,
                add_u,
                str(row["cid"]) if row.get("cid") is not None else None,
                parse_pipedrive_ts(row.get("add_time")),
                parse_pipedrive_ts(row.get("update_time")),
            ),
        )


def upsert_project_dm(
    conn: psycopg.Connection,
    row: dict[str, Any],
    *,
    custom_rows: list[tuple[str, str, Any]],
) -> None:
    prid = _safe_int(row.get("id"))
    if prid is None:
        return
    owner_id = upsert_pipedrive_user(conn, row.get("owner_id"))
    title = row.get("title") or row.get("name")
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipedrive_dm.project (
                id, title, status, owner_user_id, pipeline_id, phase_id,
                start_date, end_date, description, add_time, update_time
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                title = EXCLUDED.title, status = EXCLUDED.status, owner_user_id = EXCLUDED.owner_user_id,
                pipeline_id = EXCLUDED.pipeline_id, phase_id = EXCLUDED.phase_id,
                start_date = EXCLUDED.start_date, end_date = EXCLUDED.end_date,
                description = EXCLUDED.description, add_time = EXCLUDED.add_time,
                update_time = EXCLUDED.update_time, synced_at = NOW();
            """,
            (
                prid,
                title,
                str(row["status"]) if row.get("status") is not None else None,
                owner_id,
                _safe_int(row.get("pipeline_id")),
                _safe_int(row.get("phase_id")),
                _to_date(row.get("start_date")),
                _to_date(row.get("end_date")),
                row.get("description"),
                parse_pipedrive_ts(row.get("add_time")),
                parse_pipedrive_ts(row.get("update_time")),
            ),
        )
    replace_custom_fields(conn, entity_type="project", entity_id=str(prid), rows=custom_rows)

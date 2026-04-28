from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

import psycopg

from src.db import parse_pipedrive_ts


def _safe_int(val: Any) -> int | None:
    if val is None:
        return None
    try:
        return int(val)
    except (TypeError, ValueError):
        try:
            return int(float(val))
        except (TypeError, ValueError):
            return None


def _to_date(val: Any) -> date | None:
    if val is None or val == "":
        return None
    if isinstance(val, date) and not isinstance(val, datetime):
        return val
    dt = parse_pipedrive_ts(val)
    if dt:
        return dt.date()
    if isinstance(val, str) and len(val) >= 10:
        try:
            return date.fromisoformat(val[:10])
        except ValueError:
            return None
    return None


def _ref_id(val: Any) -> int | None:
    if val is None:
        return None
    if isinstance(val, bool):
        return None
    if isinstance(val, int):
        return val
    if isinstance(val, dict):
        v = val.get("value")
        if v is not None:
            try:
                return int(v)
            except (TypeError, ValueError):
                pass
        i = val.get("id")
        if i is not None:
            try:
                return int(i)
            except (TypeError, ValueError):
                return None
    return None


def _primary_from_list(items: Any, key: str = "value") -> str | None:
    if not items or not isinstance(items, list):
        return None
    for x in items:
        if isinstance(x, dict) and x.get("primary"):
            v = x.get(key)
            if v:
                return str(v).strip() or None
    first = items[0]
    if isinstance(first, dict):
        v = first.get(key)
        if v:
            return str(v).strip() or None
    return None


def upsert_pipedrive_user(conn: psycopg.Connection, owner: Any) -> int | None:
    if owner is None:
        return None
    if isinstance(owner, int):
        uid = owner
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipedrive_dm.pipedrive_user (id) VALUES (%s)
                ON CONFLICT (id) DO UPDATE SET synced_at = NOW();
                """,
                (uid,),
            )
        return uid
    if not isinstance(owner, dict):
        return None
    uid = _ref_id(owner)
    if uid is None:
        return None
    name = owner.get("name")
    email = owner.get("email")
    active = owner.get("active_flag")
    has_pic = owner.get("has_pic")
    pic_hash = owner.get("pic_hash")
    if isinstance(has_pic, bool):
        has_pic = 1 if has_pic else 0
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipedrive_dm.pipedrive_user (
                id, name, email, active_flag, has_pic, pic_hash
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                email = EXCLUDED.email,
                active_flag = EXCLUDED.active_flag,
                has_pic = EXCLUDED.has_pic,
                pic_hash = EXCLUDED.pic_hash,
                synced_at = NOW();
            """,
            (uid, name, email, active, has_pic, str(pic_hash) if pic_hash else None),
        )
    return uid


def upsert_organization_from_row(conn: psycopg.Connection, row: dict[str, Any]) -> int | None:
    oid = row.get("id")
    if oid is None:
        return None
    oid = int(oid)
    owner_id = upsert_pipedrive_user(conn, row.get("owner_id"))
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipedrive_dm.organization (
                id, name, owner_user_id, people_count, cc_email, address,
                active_flag, add_time, update_time
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                owner_user_id = EXCLUDED.owner_user_id,
                people_count = EXCLUDED.people_count,
                cc_email = EXCLUDED.cc_email,
                address = EXCLUDED.address,
                active_flag = EXCLUDED.active_flag,
                add_time = EXCLUDED.add_time,
                update_time = EXCLUDED.update_time,
                synced_at = NOW();
            """,
            (
                oid,
                row.get("name"),
                owner_id,
                row.get("people_count"),
                row.get("cc_email"),
                row.get("address"),
                row.get("active_flag"),
                parse_pipedrive_ts(row.get("add_time")),
                parse_pipedrive_ts(row.get("update_time")),
            ),
        )
    return oid


def upsert_organization_dm(
    conn: psycopg.Connection,
    row: dict[str, Any],
    *,
    custom_rows: list[tuple[str, str, Any]],
) -> None:
    oid = upsert_organization_from_row(conn, row)
    if oid is not None:
        replace_custom_fields(conn, entity_type="organization", entity_id=str(oid), rows=custom_rows)


def upsert_organization_embedded(conn: psycopg.Connection, org_val: Any) -> int | None:
    if org_val is None:
        return None
    if isinstance(org_val, int):
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipedrive_dm.organization (id) VALUES (%s)
                ON CONFLICT (id) DO UPDATE SET synced_at = NOW();
                """,
                (org_val,),
            )
        return org_val
    if not isinstance(org_val, dict):
        return None
    oid = _ref_id(org_val)
    if oid is None:
        return None
    owner_id = upsert_pipedrive_user(conn, org_val.get("owner_id"))
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipedrive_dm.organization (
                id, name, owner_user_id, people_count, cc_email, address,
                active_flag
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                owner_user_id = EXCLUDED.owner_user_id,
                people_count = EXCLUDED.people_count,
                cc_email = EXCLUDED.cc_email,
                address = EXCLUDED.address,
                active_flag = EXCLUDED.active_flag,
                synced_at = NOW();
            """,
            (
                oid,
                org_val.get("name"),
                owner_id,
                org_val.get("people_count"),
                org_val.get("cc_email"),
                org_val.get("address"),
                org_val.get("active_flag"),
            ),
        )
    return oid


def replace_custom_fields(
    conn: psycopg.Connection,
    *,
    entity_type: str,
    entity_id: str,
    rows: list[tuple[str, str, Any]],
) -> None:
    batch: list[tuple[str, str, str, str, str | None, Any | None]] = []
    for field_key, field_name, val in rows:
        v_text: str | None
        v_json: Any
        if isinstance(val, (dict, list)):
            v_text = None
            v_json = psycopg.types.json.Jsonb(val)
        else:
            v_text = str(val) if val is not None else None
            v_json = None
        batch.append((entity_type, entity_id, field_key, field_name, v_text, v_json))
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM pipedrive_dm.custom_field_value
            WHERE entity_type = %s AND entity_id = %s;
            """,
            (entity_type, entity_id),
        )
        if batch:
            cur.executemany(
                """
                INSERT INTO pipedrive_dm.custom_field_value (
                    entity_type, entity_id, field_key, field_name, value_text, value_json
                ) VALUES (%s, %s, %s, %s, %s, %s);
                """,
                batch,
            )


def upsert_person_dm(
    conn: psycopg.Connection,
    row: dict[str, Any],
    *,
    custom_rows: list[tuple[str, str, Any]],
) -> None:
    pid = _safe_int(row.get("id"))
    if pid is None:
        return

    org_id = upsert_organization_embedded(conn, row.get("org_id"))
    owner_id = upsert_pipedrive_user(conn, row.get("owner_id"))

    label_ids = row.get("label_ids")
    if label_ids is not None and not isinstance(label_ids, list):
        label_ids = None
    elif label_ids:
        label_ids = [int(x) for x in label_ids if x is not None]

    last_d = _to_date(row.get("last_activity_date"))
    next_d = _to_date(row.get("next_activity_date"))

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipedrive_dm.person (
                id, name, first_name, last_name, primary_email, primary_phone, job_title,
                org_id, owner_user_id, label, label_ids, visible_to, active_flag,
                won_deals_count, lost_deals_count, open_deals_count, closed_deals_count,
                related_won_deals_count, related_lost_deals_count, related_open_deals_count,
                related_closed_deals_count, participant_open_deals_count, participant_closed_deals_count,
                done_activities_count, undone_activities_count, activities_count,
                email_messages_count, files_count, notes_count, followers_count,
                last_outgoing_mail_time, last_incoming_mail_time,
                last_activity_id, next_activity_id, last_activity_date, next_activity_date,
                company_id, add_time, update_time
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                primary_email = EXCLUDED.primary_email,
                primary_phone = EXCLUDED.primary_phone,
                job_title = EXCLUDED.job_title,
                org_id = EXCLUDED.org_id,
                owner_user_id = EXCLUDED.owner_user_id,
                label = EXCLUDED.label,
                label_ids = EXCLUDED.label_ids,
                visible_to = EXCLUDED.visible_to,
                active_flag = EXCLUDED.active_flag,
                won_deals_count = EXCLUDED.won_deals_count,
                lost_deals_count = EXCLUDED.lost_deals_count,
                open_deals_count = EXCLUDED.open_deals_count,
                closed_deals_count = EXCLUDED.closed_deals_count,
                related_won_deals_count = EXCLUDED.related_won_deals_count,
                related_lost_deals_count = EXCLUDED.related_lost_deals_count,
                related_open_deals_count = EXCLUDED.related_open_deals_count,
                related_closed_deals_count = EXCLUDED.related_closed_deals_count,
                participant_open_deals_count = EXCLUDED.participant_open_deals_count,
                participant_closed_deals_count = EXCLUDED.participant_closed_deals_count,
                done_activities_count = EXCLUDED.done_activities_count,
                undone_activities_count = EXCLUDED.undone_activities_count,
                activities_count = EXCLUDED.activities_count,
                email_messages_count = EXCLUDED.email_messages_count,
                files_count = EXCLUDED.files_count,
                notes_count = EXCLUDED.notes_count,
                followers_count = EXCLUDED.followers_count,
                last_outgoing_mail_time = EXCLUDED.last_outgoing_mail_time,
                last_incoming_mail_time = EXCLUDED.last_incoming_mail_time,
                last_activity_id = EXCLUDED.last_activity_id,
                next_activity_id = EXCLUDED.next_activity_id,
                last_activity_date = EXCLUDED.last_activity_date,
                next_activity_date = EXCLUDED.next_activity_date,
                company_id = EXCLUDED.company_id,
                add_time = EXCLUDED.add_time,
                update_time = EXCLUDED.update_time,
                synced_at = NOW();
            """,
            (
                pid,
                row.get("name"),
                row.get("first_name"),
                row.get("last_name"),
                row.get("primary_email") or _primary_from_list(row.get("email")),
                _primary_from_list(row.get("phone")),
                row.get("job_title"),
                org_id,
                owner_id,
                _safe_int(row.get("label")),
                label_ids,
                str(row["visible_to"]) if row.get("visible_to") is not None else None,
                row.get("active_flag"),
                row.get("won_deals_count"),
                row.get("lost_deals_count"),
                row.get("open_deals_count"),
                row.get("closed_deals_count"),
                row.get("related_won_deals_count"),
                row.get("related_lost_deals_count"),
                row.get("related_open_deals_count"),
                row.get("related_closed_deals_count"),
                row.get("participant_open_deals_count"),
                row.get("participant_closed_deals_count"),
                row.get("done_activities_count"),
                row.get("undone_activities_count"),
                row.get("activities_count"),
                row.get("email_messages_count"),
                row.get("files_count"),
                row.get("notes_count"),
                row.get("followers_count"),
                parse_pipedrive_ts(row.get("last_outgoing_mail_time")),
                parse_pipedrive_ts(row.get("last_incoming_mail_time")),
                _safe_int(row.get("last_activity_id")),
                _safe_int(row.get("next_activity_id")),
                last_d,
                next_d,
                _safe_int(row.get("company_id")),
                parse_pipedrive_ts(row.get("add_time")),
                parse_pipedrive_ts(row.get("update_time")),
            ),
        )

    replace_custom_fields(conn, entity_type="person", entity_id=str(pid), rows=custom_rows)


def upsert_deal_dm(
    conn: psycopg.Connection,
    row: dict[str, Any],
    *,
    custom_rows: list[tuple[str, str, Any]],
) -> None:
    did = _safe_int(row.get("id"))
    if did is None:
        return

    org_id = upsert_organization_embedded(conn, row.get("org_id"))
    owner_id = upsert_pipedrive_user(conn, row.get("user_id") or row.get("owner_id"))
    person_raw = row.get("person_id")
    person_id = _ref_id(person_raw) if person_raw is not None else None

    val = row.get("value")
    if val is not None and not isinstance(val, Decimal):
        try:
            val = Decimal(str(val))
        except Exception:
            val = None

    prob = row.get("probability")
    if prob is not None:
        try:
            prob = Decimal(str(prob))
        except Exception:
            prob = None

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipedrive_dm.deal (
                id, title, value, currency, stage_id, pipeline_id, person_id, org_id,
                status, probability, owner_user_id, visible_to, lost_reason,
                expected_close_date, add_time, update_time
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                title = EXCLUDED.title,
                value = EXCLUDED.value,
                currency = EXCLUDED.currency,
                stage_id = EXCLUDED.stage_id,
                pipeline_id = EXCLUDED.pipeline_id,
                person_id = EXCLUDED.person_id,
                org_id = EXCLUDED.org_id,
                status = EXCLUDED.status,
                probability = EXCLUDED.probability,
                owner_user_id = EXCLUDED.owner_user_id,
                visible_to = EXCLUDED.visible_to,
                lost_reason = EXCLUDED.lost_reason,
                expected_close_date = EXCLUDED.expected_close_date,
                add_time = EXCLUDED.add_time,
                update_time = EXCLUDED.update_time,
                synced_at = NOW();
            """,
            (
                did,
                row.get("title"),
                val,
                row.get("currency"),
                _safe_int(row.get("stage_id")),
                _safe_int(row.get("pipeline_id")),
                person_id,
                org_id,
                str(row["status"]) if row.get("status") is not None else None,
                prob,
                owner_id,
                str(row["visible_to"]) if row.get("visible_to") is not None else None,
                row.get("lost_reason"),
                _to_date(row.get("expected_close_date")),
                parse_pipedrive_ts(row.get("add_time")),
                parse_pipedrive_ts(row.get("update_time")),
            ),
        )

    replace_custom_fields(conn, entity_type="deal", entity_id=str(did), rows=custom_rows)

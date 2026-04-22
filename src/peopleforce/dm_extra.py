"""Upsert в peopleforce_dm для list-эндпоинтов API v3 (расширение 010)."""
from __future__ import annotations

from datetime import date, datetime
from typing import Any, Callable

import psycopg
from psycopg.types.json import Jsonb

from src.peopleforce.parse import _to_bool, _to_text, _parse_date


def _first_text(row: dict[str, Any], keys: tuple[str, ...]) -> str | None:
    for k in keys:
        t = _to_text(row.get(k))
        if t:
            return t
    return None


def _parse_timestamptz(x: Any) -> Any:
    if x is None or x == "":
        return None
    if isinstance(x, datetime):
        return x
    if isinstance(x, dict):
        for k in ("at", "value", "datetime", "date_time"):
            if k in x and x[k] is not None:
                return _parse_timestamptz(x[k])
        return None
    s = str(x).strip()
    if not s:
        return None
    s = s.replace("Z", "+00:00")
    try:
        r = datetime.fromisoformat(s)
        return r
    except ValueError:
        pass
    if len(s) >= 10:
        try:
            d = _parse_date(s[:10])
            if d is not None:
                return datetime(d.year, d.month, d.day)
        except Exception:
            return None
    return None


def _to_str_list(val: Any) -> list[str] | None:
    if not isinstance(val, list) or not val:
        return None
    r = [t for t in (_to_text(x) for x in val) if t]
    return r if r else None


def _nested_name_list(val: Any) -> list[str] | None:
    """tags / skills: строки или объекты с name / title / label."""
    if not isinstance(val, list) or not val:
        return None
    out: list[str] = []
    for x in val:
        if isinstance(x, str):
            t = _to_text(x)
        elif isinstance(x, dict):
            t = _first_text(
                x,
                ("name", "title", "label", "skill", "value"),
            )
        else:
            t = _to_text(x)
        if t:
            out.append(t)
    return out if out else None


def _to_int_id(val: Any) -> int | None:
    if val is None:
        return None
    if isinstance(val, bool):
        return None
    if isinstance(val, int):
        return val
    if isinstance(val, float):
        return int(val)
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


def _require_id(row: dict[str, Any]) -> int | None:
    eid = row.get("id")
    if eid is None:
        return None
    if isinstance(eid, dict):
        eid = eid.get("id", eid.get("value"))
    try:
        return int(eid)
    except (TypeError, ValueError):
        return None


def _team_lead_ids(row: dict[str, Any]) -> tuple[int | None, str | None, str | None]:
    tl = row.get("team_lead")
    if not isinstance(tl, dict):
        return None, None, None
    tid = _to_int_id(tl.get("id"))
    em = _to_text(tl.get("email"))
    fn = _to_text(
        tl.get("full_name") or tl.get("name") or tl.get("label"),
    )
    return tid, em, fn


def upsert_department_level_row(cur: psycopg.Cursor, row: dict[str, Any]) -> bool:
    eid = _require_id(row)
    if eid is None:
        return False
    name = _first_text(row, ("name", "title", "label"))
    so = row.get("sort_order")
    if so is not None and not isinstance(so, int):
        try:
            so = int(so)
        except (TypeError, ValueError):
            so = None
    ca, ua = _parse_timestamptz(row.get("created_at")), _parse_timestamptz(
        row.get("updated_at")
    )
    cur.execute(
        """
        INSERT INTO peopleforce_dm.department_level (
            id, name, sort_order, created_at, updated_at, payload, synced_at
        ) VALUES (%s, %s, %s, %s, %s, %s::jsonb, NOW())
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name, sort_order = EXCLUDED.sort_order,
            created_at = EXCLUDED.created_at, updated_at = EXCLUDED.updated_at,
            payload = EXCLUDED.payload, synced_at = NOW()
        """,
        (eid, name, so, ca, ua, Jsonb(row)),
    )
    return True


def upsert_team_row(cur: psycopg.Cursor, row: dict[str, Any]) -> bool:
    eid = _require_id(row)
    if eid is None:
        return False
    name = _first_text(row, ("name", "title", "label"))
    tl_id, tl_em, tl_fn = _team_lead_ids(row)
    ca, ua = _parse_timestamptz(row.get("created_at")), _parse_timestamptz(
        row.get("updated_at")
    )
    cur.execute(
        """
        INSERT INTO peopleforce_dm.team (
            id, name, team_lead_id, team_lead_email, team_lead_full_name,
            created_at, updated_at, payload, synced_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, NOW())
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name, team_lead_id = EXCLUDED.team_lead_id,
            team_lead_email = EXCLUDED.team_lead_email,
            team_lead_full_name = EXCLUDED.team_lead_full_name,
            created_at = EXCLUDED.created_at, updated_at = EXCLUDED.updated_at,
            payload = EXCLUDED.payload, synced_at = NOW()
        """,
        (eid, name, tl_id, tl_em, tl_fn, ca, ua, Jsonb(row)),
    )
    return True


def upsert_leave_type_row(cur: psycopg.Cursor, row: dict[str, Any]) -> bool:
    eid = _require_id(row)
    if eid is None:
        return False
    name = _first_text(row, ("name", "title", "label"))
    ca, ua = _parse_timestamptz(row.get("created_at")), _parse_timestamptz(
        row.get("updated_at")
    )
    cur.execute(
        """
        INSERT INTO peopleforce_dm.leave_type (
            id, name, unit, fa_class, hex_color, created_at, updated_at, payload, synced_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, NOW())
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name, unit = EXCLUDED.unit, fa_class = EXCLUDED.fa_class,
            hex_color = EXCLUDED.hex_color, created_at = EXCLUDED.created_at,
            updated_at = EXCLUDED.updated_at, payload = EXCLUDED.payload, synced_at = NOW()
        """,
        (
            eid,
            name,
            _to_text(row.get("unit")),
            _to_text(row.get("fa_class")),
            _to_text(row.get("hex_color")),
            ca,
            ua,
            Jsonb(row),
        ),
    )
    return True


def _upsert_name_ts_payload(
    cur: psycopg.Cursor,
    table: str,
    row: dict[str, Any],
    *,
    name_keys: tuple[str, ...] = ("name", "title", "label", "full_name"),
) -> bool:
    eid = _require_id(row)
    if eid is None:
        return False
    name = _first_text(row, name_keys)
    ca, ua = _parse_timestamptz(row.get("created_at")), _parse_timestamptz(
        row.get("updated_at")
    )
    cur.execute(
        f"""
        INSERT INTO peopleforce_dm.{table} (id, name, created_at, updated_at, payload, synced_at)
        VALUES (%s, %s, %s, %s, %s::jsonb, NOW())
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name, created_at = EXCLUDED.created_at, updated_at = EXCLUDED.updated_at,
            payload = EXCLUDED.payload, synced_at = NOW()
        """,
        (eid, name, ca, ua, Jsonb(row)),
    )
    return True


def upsert_leave_request_row(cur: psycopg.Cursor, row: dict[str, Any]) -> bool:
    eid = _require_id(row)
    if eid is None:
        return False
    eid_emp = row.get("employee_id")
    if isinstance(eid_emp, dict):
        eid_emp = eid_emp.get("id")
    try:
        employee_id = int(eid_emp) if eid_emp is not None else None
    except (TypeError, ValueError):
        employee_id = None
    lti = row.get("leave_type_id")
    if isinstance(lti, dict):
        lti = lti.get("id")
    try:
        leave_type_id = int(lti) if lti is not None else None
    except (TypeError, ValueError):
        leave_type_id = None
    lt = row.get("leave_type")
    if isinstance(lt, dict):
        leave_type_name = _to_text(lt.get("name")) or _to_text(lt.get("label")) or _to_text(lt)
    else:
        leave_type_name = _to_text(lt)
    cur.execute(
        """
        INSERT INTO peopleforce_dm.leave_request (
            id, employee_id, leave_type_id, leave_type_name, state, amount, tracking_time_in,
            on_demand, starts_on, ends_on, comment, created_at, updated_at, payload, synced_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, NOW()
        )
        ON CONFLICT (id) DO UPDATE SET
            employee_id = EXCLUDED.employee_id, leave_type_id = EXCLUDED.leave_type_id,
            leave_type_name = EXCLUDED.leave_type_name, state = EXCLUDED.state, amount = EXCLUDED.amount,
            tracking_time_in = EXCLUDED.tracking_time_in, on_demand = EXCLUDED.on_demand,
            starts_on = EXCLUDED.starts_on, ends_on = EXCLUDED.ends_on, comment = EXCLUDED.comment,
            created_at = EXCLUDED.created_at, updated_at = EXCLUDED.updated_at,
            payload = EXCLUDED.payload, synced_at = NOW()
        """,
        (
            eid,
            employee_id,
            leave_type_id,
            leave_type_name,
            _to_text(row.get("state")),
            row.get("amount") if isinstance(row.get("amount"), (int, float)) else None,
            _to_text(row.get("tracking_time_in")),
            _to_bool(row.get("on_demand")),
            _parse_date(row.get("starts_on")),
            _parse_date(row.get("ends_on")),
            _to_text(row.get("comment")),
            _parse_timestamptz(row.get("created_at")),
            _parse_timestamptz(row.get("updated_at")),
            Jsonb(row),
        ),
    )
    return True


def _holiday_event_date(row: dict[str, Any]) -> date | None:
    for k in (
        "date",
        "on",
        "holiday_date",
        "event_date",
        "day",
    ):
        d = _parse_date(row.get(k))
        if d is not None:
            return d
    return None


def upsert_public_holiday_row(cur: psycopg.Cursor, row: dict[str, Any]) -> bool:
    eid = _require_id(row)
    if eid is None:
        return False
    name = _first_text(row, ("name", "title", "label"))
    ed = _holiday_event_date(row)
    cc = _to_text(
        row.get("country_code") or row.get("country") or row.get("region"),
    )
    ca, ua = _parse_timestamptz(row.get("created_at")), _parse_timestamptz(
        row.get("updated_at")
    )
    cur.execute(
        """
        INSERT INTO peopleforce_dm.public_holiday (
            id, name, event_date, country_code, created_at, updated_at, payload, synced_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, NOW())
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name, event_date = EXCLUDED.event_date, country_code = EXCLUDED.country_code,
            created_at = EXCLUDED.created_at, updated_at = EXCLUDED.updated_at,
            payload = EXCLUDED.payload, synced_at = NOW()
        """,
        (eid, name, ed, cc, ca, ua, Jsonb(row)),
    )
    return True


def upsert_company_holiday_row(cur: psycopg.Cursor, row: dict[str, Any]) -> bool:
    eid = _require_id(row)
    if eid is None:
        return False
    name = _first_text(row, ("name", "title", "label"))
    ed = _holiday_event_date(row)
    ca, ua = _parse_timestamptz(row.get("created_at")), _parse_timestamptz(
        row.get("updated_at")
    )
    cur.execute(
        """
        INSERT INTO peopleforce_dm.company_holiday (
            id, name, event_date, created_at, updated_at, payload, synced_at
        ) VALUES (%s, %s, %s, %s, %s, %s::jsonb, NOW())
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name, event_date = EXCLUDED.event_date,
            created_at = EXCLUDED.created_at, updated_at = EXCLUDED.updated_at,
            payload = EXCLUDED.payload, synced_at = NOW()
        """,
        (eid, name, ed, ca, ua, Jsonb(row)),
    )
    return True


def _first_ts(row: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for k in keys:
        t = _parse_timestamptz(row.get(k))
        if t is not None:
            return t
    return None


def upsert_time_entry_row(cur: psycopg.Cursor, row: dict[str, Any]) -> bool:
    eid = _require_id(row)
    if eid is None:
        return False
    eid_emp = row.get("employee_id")
    if isinstance(eid_emp, dict):
        eid_emp = eid_emp.get("id")
    try:
        employee_id = int(eid_emp) if eid_emp is not None else None
    except (TypeError, ValueError):
        employee_id = None
    name = _first_text(
        row,
        ("name", "title", "label", "description", "summary"),
    )
    started = _first_ts(
        row,
        (
            "started_at",
            "starts_at",
            "from",
            "start_time",
        ),
    )
    ended = _first_ts(
        row,
        (
            "ended_at",
            "ends_at",
            "to",
            "end_time",
        ),
    )
    rec_cr = _parse_timestamptz(row.get("created_at"))
    rec_up = _parse_timestamptz(row.get("updated_at"))
    cur.execute(
        """
        INSERT INTO peopleforce_dm.time_entry (
            id, name, employee_id, started_at, ended_at, created_at, updated_at, payload, synced_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, NOW())
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name, employee_id = EXCLUDED.employee_id, started_at = EXCLUDED.started_at,
            ended_at = EXCLUDED.ended_at, created_at = EXCLUDED.created_at, updated_at = EXCLUDED.updated_at,
            payload = EXCLUDED.payload, synced_at = NOW()
        """,
        (eid, name, employee_id, started, ended, rec_cr, rec_up, Jsonb(row)),
    )
    return True


def upsert_recruitment_vacancy_row(cur: psycopg.Cursor, row: dict[str, Any]) -> bool:
    eid = _require_id(row)
    if eid is None:
        return False
    title = _to_text(row.get("title"))
    name = title or _first_text(row, ("name", "label"))
    state = _to_text(row.get("state") or row.get("status") or row.get("stage"))
    desc = _to_text(
        row.get("description")
        or row.get("summary")
        or (row.get("body") if isinstance(row.get("body"), str) else None),
    )
    tags = _nested_name_list(row.get("tags"))
    skills = _nested_name_list(row.get("skills"))
    ca, ua = _parse_timestamptz(row.get("created_at")), _parse_timestamptz(
        row.get("updated_at")
    )
    cur.execute(
        """
        INSERT INTO peopleforce_dm.recruitment_vacancy (
            id, name, state, title, description, tags, skills, created_at, updated_at, payload, synced_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, NOW())
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name, state = EXCLUDED.state, title = EXCLUDED.title,
            description = EXCLUDED.description, tags = EXCLUDED.tags, skills = EXCLUDED.skills,
            created_at = EXCLUDED.created_at, updated_at = EXCLUDED.updated_at,
            payload = EXCLUDED.payload, synced_at = NOW()
        """,
        (eid, name, state, title, desc, tags, skills, ca, ua, Jsonb(row)),
    )
    return True


def upsert_recruitment_candidate_row(cur: psycopg.Cursor, row: dict[str, Any]) -> bool:
    eid = _require_id(row)
    if eid is None:
        return False
    full_name = _to_text(row.get("full_name"))
    name = full_name or _first_text(row, ("name", "title", "label"))
    em: Any = row.get("email")
    if not em and isinstance(row.get("emails"), list) and row["emails"]:
        em = row["emails"][0]
    email = _to_text(em)
    cb = row.get("created_by")
    created_by_json = Jsonb(cb) if isinstance(cb, dict) else None
    apps = row.get("applications")
    applications_json = Jsonb(apps) if isinstance(apps, list) else None
    cur.execute(
        """
        INSERT INTO peopleforce_dm.recruitment_candidate (
            id, name, full_name, email, tags, urls, level, gender, gender_id, resume, skills,
            source, location, position, cover_letter, date_of_birth, phone_numbers,
            created_at, updated_at, created_by, applications, payload, synced_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            NOW()
        )
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name, full_name = EXCLUDED.full_name, email = EXCLUDED.email,
            tags = EXCLUDED.tags, urls = EXCLUDED.urls, level = EXCLUDED.level, gender = EXCLUDED.gender,
            gender_id = EXCLUDED.gender_id, resume = EXCLUDED.resume, skills = EXCLUDED.skills,
            source = EXCLUDED.source, location = EXCLUDED.location, position = EXCLUDED.position,
            cover_letter = EXCLUDED.cover_letter, date_of_birth = EXCLUDED.date_of_birth,
            phone_numbers = EXCLUDED.phone_numbers, created_at = EXCLUDED.created_at,
            updated_at = EXCLUDED.updated_at, created_by = EXCLUDED.created_by,
            applications = EXCLUDED.applications, payload = EXCLUDED.payload, synced_at = NOW()
        """,
        (
            eid,
            name,
            full_name,
            email,
            _to_str_list(row.get("tags")),
            _to_str_list(row.get("urls")),
            _to_text(row.get("level")),
            _to_text(row.get("gender")),
            _to_int_id(row.get("gender_id")),
            _to_bool(row.get("resume")),
            _to_str_list(row.get("skills")),
            _to_text(row.get("source")),
            _to_text(row.get("location")),
            _to_text(row.get("position")),
            _to_text(row.get("cover_letter")),
            _parse_date(row.get("date_of_birth")),
            _to_str_list(row.get("phone_numbers")),
            _parse_timestamptz(row.get("created_at")),
            _parse_timestamptz(row.get("updated_at")),
            created_by_json,
            applications_json,
            Jsonb(row),
        ),
    )
    return True


def upsert_recruitment_application_row(cur: psycopg.Cursor, row: dict[str, Any]) -> bool:
    eid = _require_id(row)
    if eid is None:
        return False
    name = _first_text(row, ("name", "title", "label"))
    vid = row.get("vacancy_id")
    cid = row.get("candidate_id")
    for key, v in (("vacancy", vid), ("candidate", cid)):
        if v is None and isinstance(row.get(key), dict):
            v = (row.get(key) or {}).get("id")
        if key == "vacancy":
            vid = v
        else:
            cid = v
    try:
        vacancy_id = int(vid) if vid is not None else None
    except (TypeError, ValueError):
        vacancy_id = None
    try:
        candidate_id = int(cid) if cid is not None else None
    except (TypeError, ValueError):
        candidate_id = None
    st = _to_text(row.get("state") or row.get("status") or row.get("stage"))
    ca, ua = _parse_timestamptz(row.get("created_at")), _parse_timestamptz(
        row.get("updated_at")
    )
    ps = row.get("pipeline_state")
    pipeline_state_id = None
    pipeline_state_name = None
    if isinstance(ps, dict):
        pipeline_state_id = _to_int_id(ps.get("id"))
        pipeline_state_name = _to_text(ps.get("name"))
    dq = row.get("disqualify_reason")
    dqn = _to_text(dq.get("name")) if isinstance(dq, dict) else None
    disq_at = _parse_timestamptz(row.get("disqualified_at"))
    cur.execute(
        """
        INSERT INTO peopleforce_dm.recruitment_application (
            id, name, vacancy_id, candidate_id, state, created_at, updated_at,
            pipeline_state_id, pipeline_state_name, disqualified_at, disqualify_reason_name,
            payload, synced_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, NOW()
        )
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name, vacancy_id = EXCLUDED.vacancy_id, candidate_id = EXCLUDED.candidate_id,
            state = EXCLUDED.state, created_at = EXCLUDED.created_at, updated_at = EXCLUDED.updated_at,
            pipeline_state_id = EXCLUDED.pipeline_state_id, pipeline_state_name = EXCLUDED.pipeline_state_name,
            disqualified_at = EXCLUDED.disqualified_at, disqualify_reason_name = EXCLUDED.disqualify_reason_name,
            payload = EXCLUDED.payload, synced_at = NOW()
        """,
        (
            eid,
            name,
            vacancy_id,
            candidate_id,
            st,
            ca,
            ua,
            pipeline_state_id,
            pipeline_state_name,
            disq_at,
            dqn,
            Jsonb(row),
        ),
    )
    return True


def upsert_cost_center_row(cur: psycopg.Cursor, row: dict[str, Any]) -> bool:
    eid = _require_id(row)
    if eid is None:
        return False
    name = _first_text(row, ("name", "title", "label"))
    code = _to_text(row.get("code") or row.get("number") or row.get("key"))
    p = row.get("parent_id")
    if p is None and isinstance(row.get("parent"), dict):
        p = (row.get("parent") or {}).get("id")
    parent_id = _to_int_id(p)
    ca, ua = _parse_timestamptz(row.get("created_at")), _parse_timestamptz(
        row.get("updated_at")
    )
    cur.execute(
        """
        INSERT INTO peopleforce_dm.cost_center (
            id, name, code, parent_id, created_at, updated_at, payload, synced_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, NOW())
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name, code = EXCLUDED.code, parent_id = EXCLUDED.parent_id,
            created_at = EXCLUDED.created_at, updated_at = EXCLUDED.updated_at,
            payload = EXCLUDED.payload, synced_at = NOW()
        """,
        (eid, name, code, parent_id, ca, ua, Jsonb(row)),
    )
    return True


def upsert_employee_custom_field_row(cur: psycopg.Cursor, row: dict[str, Any]) -> bool:
    eid = _require_id(row)
    if eid is None:
        return False
    name = _first_text(row, ("name", "label", "title"))
    fk = _to_text(
        row.get("field_key")
        or row.get("key")
        or row.get("attribute")
        or row.get("code")
    )
    data_type = _to_text(
        row.get("data_type")
        or row.get("type")
        or row.get("field_type")
    )
    pos = row.get("position")
    if pos is not None and not isinstance(pos, int):
        try:
            pos = int(pos)
        except (TypeError, ValueError):
            pos = None
    ca, ua = _parse_timestamptz(row.get("created_at")), _parse_timestamptz(
        row.get("updated_at")
    )
    cur.execute(
        """
        INSERT INTO peopleforce_dm.employee_custom_field (
            id, name, field_key, data_type, field_position, required, created_at, updated_at, payload, synced_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, NOW())
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name, field_key = EXCLUDED.field_key, data_type = EXCLUDED.data_type,
            field_position = EXCLUDED.field_position, required = EXCLUDED.required,
            created_at = EXCLUDED.created_at, updated_at = EXCLUDED.updated_at,
            payload = EXCLUDED.payload, synced_at = NOW()
        """,
        (
            eid,
            name,
            fk,
            data_type,
            pos,
            _to_bool(row.get("required") or row.get("mandatory")),
            ca,
            ua,
            Jsonb(row),
        ),
    )
    return True


def upsert_asset_row(cur: psycopg.Cursor, row: dict[str, Any]) -> bool:
    eid = _require_id(row)
    if eid is None:
        return False
    name = _first_text(row, ("name", "title", "label"))
    sn = _to_text(row.get("serial_number") or row.get("serial"))
    at = _to_text(row.get("asset_type") or row.get("type") or row.get("category"))
    st = _to_text(row.get("status") or row.get("state"))
    ca, ua = _parse_timestamptz(row.get("created_at")), _parse_timestamptz(
        row.get("updated_at")
    )
    cur.execute(
        """
        INSERT INTO peopleforce_dm.asset (
            id, name, serial_number, asset_type, status, created_at, updated_at, payload, synced_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, NOW())
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name, serial_number = EXCLUDED.serial_number, asset_type = EXCLUDED.asset_type,
            status = EXCLUDED.status, created_at = EXCLUDED.created_at, updated_at = EXCLUDED.updated_at,
            payload = EXCLUDED.payload, synced_at = NOW()
        """,
        (eid, name, sn, at, st, ca, ua, Jsonb(row)),
    )
    return True


def upsert_document_type_row(cur: psycopg.Cursor, row: dict[str, Any]) -> bool:
    eid = _require_id(row)
    if eid is None:
        return False
    name = _first_text(row, ("name", "title", "label"))
    cat = _to_text(
        row.get("category")
        or row.get("group")
        or (row.get("type") if isinstance(row.get("type"), str) else None),
    )
    ca, ua = _parse_timestamptz(row.get("created_at")), _parse_timestamptz(
        row.get("updated_at")
    )
    cur.execute(
        """
        INSERT INTO peopleforce_dm.document_type (
            id, name, category, created_at, updated_at, payload, synced_at
        ) VALUES (%s, %s, %s, %s, %s, %s::jsonb, NOW())
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name, category = EXCLUDED.category,
            created_at = EXCLUDED.created_at, updated_at = EXCLUDED.updated_at,
            payload = EXCLUDED.payload, synced_at = NOW()
        """,
        (eid, name, cat, ca, ua, Jsonb(row)),
    )
    return True


def upsert_project_row(cur: psycopg.Cursor, row: dict[str, Any]) -> bool:
    eid = _require_id(row)
    if eid is None:
        return False
    name = _first_text(row, ("name", "title", "label"))
    code = _to_text(row.get("code") or row.get("key") or row.get("reference"))
    status = _to_text(row.get("status") or row.get("state"))
    ca, ua = _parse_timestamptz(row.get("created_at")), _parse_timestamptz(
        row.get("updated_at")
    )
    cur.execute(
        """
        INSERT INTO peopleforce_dm.project (
            id, name, code, status, created_at, updated_at, payload, synced_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, NOW())
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name, code = EXCLUDED.code, status = EXCLUDED.status,
            created_at = EXCLUDED.created_at, updated_at = EXCLUDED.updated_at,
            payload = EXCLUDED.payload, synced_at = NOW()
        """,
        (eid, name, code, status, ca, ua, Jsonb(row)),
    )
    return True


def upsert_work_schedule_row(cur: psycopg.Cursor, row: dict[str, Any]) -> bool:
    return _upsert_name_ts_payload(cur, "work_schedule", row)


def upsert_working_pattern_row(cur: psycopg.Cursor, row: dict[str, Any]) -> bool:
    return _upsert_name_ts_payload(cur, "working_pattern", row)


def upsert_shift_row(cur: psycopg.Cursor, row: dict[str, Any]) -> bool:
    return _upsert_name_ts_payload(cur, "shift", row)


def upsert_pay_schedule_row(cur: psycopg.Cursor, row: dict[str, Any]) -> bool:
    return _upsert_name_ts_payload(cur, "pay_schedule", row)


# Маппинг path -> (upsert_fn) для dm_extra-специфичных/общих
PATH_DM_UPSERT: dict[str, Callable[[psycopg.Cursor, dict[str, Any]], bool]] = {
    "/teams": upsert_team_row,
    "/department_levels": upsert_department_level_row,
    "/leave_types": upsert_leave_type_row,
    "/leave_requests": upsert_leave_request_row,
    "/public_holidays": upsert_public_holiday_row,
    "/company_holidays": upsert_company_holiday_row,
    "/work_schedules": upsert_work_schedule_row,
    "/working_patterns": upsert_working_pattern_row,
    "/time_entries": upsert_time_entry_row,
    "/shifts": upsert_shift_row,
    "/recruitment/vacancies": upsert_recruitment_vacancy_row,
    "/recruitment/candidates": upsert_recruitment_candidate_row,
    "/recruitment/applications": upsert_recruitment_application_row,
    "/pay_schedules": upsert_pay_schedule_row,
    "/custom_fields/employees": upsert_employee_custom_field_row,
    "/assets": upsert_asset_row,
    "/document_types": upsert_document_type_row,
    "/cost_centers": upsert_cost_center_row,
    "/projects": upsert_project_row,
}

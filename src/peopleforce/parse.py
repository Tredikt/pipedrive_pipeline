"""Нормализация JSON сотрудника (список API и webhook: attributes + data)."""

from __future__ import annotations

import json
from datetime import date
from typing import Any

import psycopg
from psycopg.types.json import Jsonb


def _to_bool(x: Any) -> bool | None:
    if x is None:
        return None
    if isinstance(x, bool):
        return x
    if isinstance(x, (int, float)):
        return bool(x)
    if isinstance(x, dict):
        for k in ("value", "active", "flag"):
            if k in x and x[k] is not None:
                return _to_bool(x[k])
        return None
    if isinstance(x, str):
        lo = x.lower()
        if lo in ("true", "1", "yes"):
            return True
        if lo in ("false", "0", "no"):
            return False
    return None


def _to_text(x: Any) -> str | None:
    """Строка для TEXT-колонок: API v3 иногда шлёт enum/объект вместо строки."""
    if x is None:
        return None
    if isinstance(x, str):
        s = x.strip()
        return s or None
    if isinstance(x, (int, float)) and not isinstance(x, bool):
        if isinstance(x, float) and x.is_integer():
            return str(int(x))
        return str(x)
    if isinstance(x, bool):
        return "true" if x else "false"
    if isinstance(x, dict):
        for k in ("value", "name", "label", "code", "email", "number", "full_number"):
            if k in x and x[k] is not None:
                return _to_text(x[k])
        return json.dumps(x, ensure_ascii=False)[:4000] or None
    if isinstance(x, list):
        if not x:
            return None
        parts = [p for p in (_to_text(i) for i in x) if p]
        return ", ".join(parts) if parts else None
    return str(x)[:4000] if x is not None else None


def _pick_scalar(src: dict[str, Any], *keys: str) -> Any:
    """Первое непустое значение по ключам (варианты snake_case / kebab / camel для PF webhook/API)."""
    for k in keys:
        if k not in src:
            continue
        v = src[k]
        if v is None or v == "":
            continue
        return v
    return None


def _parse_date(x: Any) -> date | None:
    if x is None or x == "":
        return None
    if isinstance(x, date):
        return x
    if isinstance(x, dict):
        for k in ("date", "value", "on"):
            if k in x and x[k] is not None:
                return _parse_date(x[k])
        return None
    s = str(x).strip()[:10]
    if not s or s.lower() in ("null", "none"):
        return None
    from datetime import datetime

    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        return None


def merge_webhook_employee_data(data: dict[str, Any]) -> dict[str, Any]:
    """Webhook: data.id + data.attributes + вложенные объекты."""
    out: dict[str, Any] = dict(data)
    attrs = out.pop("attributes", None)
    if isinstance(attrs, dict):
        for k, v in attrs.items():
            if k not in out or out.get(k) is None:
                out[k] = v
    return out


def flat_employee_row(src: dict[str, Any]) -> dict[str, Any]:
    """Поля для peopleforce_dm.employee."""
    s = merge_webhook_employee_data(src) if "attributes" in src else dict(src)
    pos = s.get("position") if isinstance(s.get("position"), dict) else {}
    jl = s.get("job_level") if isinstance(s.get("job_level"), dict) else {}
    loc = s.get("location") if isinstance(s.get("location"), dict) else {}
    et = s.get("employment_type") if isinstance(s.get("employment_type"), dict) else {}
    div = s.get("division") if isinstance(s.get("division"), dict) else {}
    dep = s.get("department") if isinstance(s.get("department"), dict) else {}
    rep = s.get("reporting_to") if isinstance(s.get("reporting_to"), dict) else {}
    pick_src: dict[str, Any] = dict(s)
    term_blk = s.get("termination")
    if isinstance(term_blk, dict):
        pick_src = {**term_blk, **s}
    eid = s.get("id")
    if eid is None:
        raise ValueError("employee id missing")
    if isinstance(eid, dict):
        eid = eid.get("id", eid.get("value"))
    eid_i = int(eid) if eid is not None else None
    if eid_i is None:
        raise ValueError("employee id missing")

    def _nid(nested: dict[str, Any], key: str = "id") -> int | None:
        v = nested.get(key) if nested else None
        if v is None:
            return None
        if isinstance(v, dict):
            v = v.get("id", v.get("value"))
        try:
            return int(v) if v is not None else None
        except (TypeError, ValueError):
            return None

    return {
        "id": eid_i,
        "active": _to_bool(s.get("active")),
        "access": _to_bool(s.get("access")),
        "employee_number": _to_text(s.get("employee_number")),
        "full_name": _to_text(s.get("full_name")),
        "first_name": _to_text(s.get("first_name")),
        "middle_name": _to_text(s.get("middle_name")),
        "last_name": _to_text(s.get("last_name")),
        "email": _to_text(s.get("email")),
        "personal_email": _to_text(s.get("personal_email")),
        "mobile_number": _to_text(s.get("mobile_number")),
        "work_phone_number": _to_text(s.get("work_phone_number")),
        "date_of_birth": _parse_date(s.get("date_of_birth")),
        "gender": _to_text(s.get("gender")),
        "avatar_url": _to_text(s.get("avatar_url")),
        "probation_ends_on": _parse_date(s.get("probation_ends_on")),
        "hired_on": _parse_date(s.get("hired_on")),
        "skype_username": _to_text(s.get("skype_username")),
        "slack_username": _to_text(s.get("slack_username")),
        "twitter_username": _to_text(s.get("twitter_username")),
        "facebook_url": _to_text(s.get("facebook_url")),
        "linkedin_url": _to_text(s.get("linkedin_url")),
        "position_id": _nid(pos, "id") if pos else None,
        "job_level_id": _nid(jl, "id") if jl else None,
        "location_id": _nid(loc, "id") if loc else None,
        "employment_type_id": _nid(et, "id") if et else None,
        "division_id": _nid(div, "id") if div else None,
        "department_id": _nid(dep, "id") if dep else None,
        "manager_employee_id": _nid(rep, "id") if rep else None,
        "reporting_to_id": _nid(rep, "id") if rep else None,
        "employment_status": _to_text(
            _pick_scalar(
                pick_src,
                "status",
                "employment_status",
                "employment-status",
            )
        ),
        "termination_effective_on": _parse_date(
            _pick_scalar(
                pick_src,
                "termination_effective_on",
                "termination-effective-on",
                "termination_effective_date",
                "termination-effective-date",
                "effective_from",
                "effective-from",
            )
        ),
        "terminated_on": _parse_date(
            _pick_scalar(
                pick_src,
                "terminated_on",
                "terminated-on",
                "terminatedOn",
            )
        ),
    }


# ref: вручную для position (id, name)
def upsert_nested_refs_from_employee(cur: psycopg.Cursor, raw: dict[str, Any]) -> None:
    """
    Справочники из вложенных объектов сотрудника — до upsert employee (FK).
    GET /departments и т.д. бывает неполный; у сотрудника department/division/location
    с id, которого нет в заранее выгруженном списке.
    """
    pos = raw.get("position")
    if isinstance(pos, dict) and pos.get("id"):
        upsert_position_row(cur, pos)
    jl = raw.get("job_level")
    if isinstance(jl, dict) and jl.get("id"):
        upsert_job_level_row(cur, jl)
    et = raw.get("employment_type")
    if isinstance(et, dict) and et.get("id"):
        upsert_employment_type_row(cur, et)
    dep = raw.get("department")
    if isinstance(dep, dict) and dep.get("id"):
        upsert_department_row(cur, dep)
    div = raw.get("division")
    if isinstance(div, dict) and div.get("id"):
        upsert_division_row(cur, div)
    loc = raw.get("location")
    if isinstance(loc, dict) and loc.get("id"):
        upsert_location_row(cur, loc)


def upsert_position_row(cur: psycopg.Cursor, d: dict[str, Any]) -> None:
    if not d.get("id"):
        return
    cur.execute(
        """
        INSERT INTO peopleforce_dm.position (id, name, synced_at)
        VALUES (%s, %s, NOW())
        ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, synced_at = NOW()
        """,
        (d["id"], _to_text(d.get("name"))),
    )


def upsert_job_level_row(cur: psycopg.Cursor, d: dict[str, Any]) -> None:
    if not d.get("id"):
        return
    cur.execute(
        """
        INSERT INTO peopleforce_dm.job_level (id, name, created_at, updated_at, synced_at)
        VALUES (%s, %s, %s, %s, NOW())
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            created_at = COALESCE(EXCLUDED.created_at, peopleforce_dm.job_level.created_at),
            updated_at = COALESCE(EXCLUDED.updated_at, peopleforce_dm.job_level.updated_at),
            synced_at = NOW()
        """,
        (
            d["id"],
            _to_text(d.get("name")),
            d.get("created_at"),
            d.get("updated_at"),
        ),
    )


def upsert_employment_type_row(cur: psycopg.Cursor, d: dict[str, Any]) -> None:
    if not d.get("id"):
        return
    cur.execute(
        """
        INSERT INTO peopleforce_dm.employment_type (id, name, created_at, synced_at)
        VALUES (%s, %s, %s, NOW())
        ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, created_at = COALESCE(EXCLUDED.created_at, peopleforce_dm.employment_type.created_at), synced_at = NOW()
        """,
        (d["id"], _to_text(d.get("name")), d.get("created_at")),
    )


def upsert_department_row(cur: psycopg.Cursor, d: dict[str, Any]) -> None:
    cur.execute(
        """
        INSERT INTO peopleforce_dm.department
            (id, name, parent_id, manager_id, department_level_id, synced_at)
        VALUES (%s, %s, %s, %s, %s, NOW())
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            parent_id = EXCLUDED.parent_id,
            manager_id = EXCLUDED.manager_id,
            department_level_id = EXCLUDED.department_level_id,
            synced_at = NOW()
        """,
        (
            d["id"],
            _to_text(d.get("name")),
            d.get("parent_id"),
            d.get("manager_id"),
            d.get("department_level_id"),
        ),
    )


def upsert_division_row(cur: psycopg.Cursor, d: dict[str, Any]) -> None:
    cur.execute(
        """
        INSERT INTO peopleforce_dm.division (id, name, synced_at)
        VALUES (%s, %s, NOW())
        ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, synced_at = NOW()
        """,
        (d["id"], _to_text(d.get("name"))),
    )


def upsert_location_row(cur: psycopg.Cursor, d: dict[str, Any]) -> None:
    cur.execute(
        """
        INSERT INTO peopleforce_dm.location (id, name, synced_at)
        VALUES (%s, %s, NOW())
        ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, synced_at = NOW()
        """,
        (d["id"], _to_text(d.get("name"))),
    )


def upsert_employee_row(cur: psycopg.Cursor, f: dict[str, Any]) -> None:
    cur.execute(
        """
        INSERT INTO peopleforce_dm.employee (
            id, active, access, employee_number, full_name, first_name, middle_name, last_name,
            email, personal_email, mobile_number, work_phone_number, date_of_birth, gender,
            avatar_url, probation_ends_on, hired_on, skype_username, slack_username,
            twitter_username, facebook_url, linkedin_url,
            position_id, job_level_id, location_id, employment_type_id, division_id, department_id,
            manager_employee_id, reporting_to_id,
            employment_status, termination_effective_on, terminated_on,
            synced_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s,
            NOW()
        )
        ON CONFLICT (id) DO UPDATE SET
            active = EXCLUDED.active, access = EXCLUDED.access, employee_number = EXCLUDED.employee_number,
            full_name = EXCLUDED.full_name, first_name = EXCLUDED.first_name, middle_name = EXCLUDED.middle_name,
            last_name = EXCLUDED.last_name, email = EXCLUDED.email, personal_email = EXCLUDED.personal_email,
            mobile_number = EXCLUDED.mobile_number, work_phone_number = EXCLUDED.work_phone_number,
            date_of_birth = EXCLUDED.date_of_birth, gender = EXCLUDED.gender, avatar_url = EXCLUDED.avatar_url,
            probation_ends_on = EXCLUDED.probation_ends_on, hired_on = EXCLUDED.hired_on,
            skype_username = EXCLUDED.skype_username, slack_username = EXCLUDED.slack_username,
            twitter_username = EXCLUDED.twitter_username, facebook_url = EXCLUDED.facebook_url,
            linkedin_url = EXCLUDED.linkedin_url, position_id = EXCLUDED.position_id, job_level_id = EXCLUDED.job_level_id,
            location_id = EXCLUDED.location_id, employment_type_id = EXCLUDED.employment_type_id,
            division_id = EXCLUDED.division_id, department_id = EXCLUDED.department_id,
            manager_employee_id = EXCLUDED.manager_employee_id, reporting_to_id = EXCLUDED.reporting_to_id,
            employment_status = EXCLUDED.employment_status,
            termination_effective_on = EXCLUDED.termination_effective_on,
            terminated_on = EXCLUDED.terminated_on,
            synced_at = NOW()
        """,
        (
            f["id"],
            f.get("active"),
            f.get("access"),
            f.get("employee_number"),
            f.get("full_name"),
            f.get("first_name"),
            f.get("middle_name"),
            f.get("last_name"),
            f.get("email"),
            f.get("personal_email"),
            f.get("mobile_number"),
            f.get("work_phone_number"),
            f.get("date_of_birth"),
            f.get("gender"),
            f.get("avatar_url"),
            f.get("probation_ends_on"),
            f.get("hired_on"),
            f.get("skype_username"),
            f.get("slack_username"),
            f.get("twitter_username"),
            f.get("facebook_url"),
            f.get("linkedin_url"),
            f.get("position_id"),
            f.get("job_level_id"),
            f.get("location_id"),
            f.get("employment_type_id"),
            f.get("division_id"),
            f.get("department_id"),
            f.get("manager_employee_id"),
            f.get("reporting_to_id"),
            f.get("employment_status"),
            f.get("termination_effective_on"),
            f.get("terminated_on"),
        ),
    )


def upsert_entity_record(
    cur: psycopg.Cursor,
    *,
    entity_type: str,
    external_id: str,
    raw: dict[str, Any],
) -> None:
    cur.execute(
        """
        INSERT INTO peopleforce_raw.entity_record (entity_type, external_id, raw, synced_at)
        VALUES (%s, %s, %s::jsonb, NOW())
        ON CONFLICT (entity_type, external_id) DO UPDATE SET
            raw = EXCLUDED.raw, synced_at = NOW()
        """,
        (entity_type, external_id, Jsonb(raw)),
    )

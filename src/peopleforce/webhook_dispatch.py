"""
Обработка вебхуков PeopleForce по списку Topics (Webhooks overview).

https://developer.peopleforce.io/docs/webhooks
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
from typing import Any

import psycopg
from fastapi import HTTPException

from src.peopleforce.dm_extra import (
    upsert_leave_request_row,
    upsert_recruitment_application_row,
    upsert_recruitment_candidate_row,
    upsert_recruitment_vacancy_row,
)
from src.master_link import link_master_after_pf_employee_upsert
from src.peopleforce.parse import (
    flat_employee_row,
    merge_webhook_employee_data,
    upsert_employee_row,
    upsert_entity_record,
    upsert_nested_refs_from_employee,
)

logger = logging.getLogger(__name__)


def _entity_type_safe(action: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9_]+", "_", action.lower()).strip("_")
    return s or "webhook_event"


def _int_id(data: dict[str, Any], *keys: str) -> int | None:
    for k in keys:
        v = data.get(k)
        if v is None:
            continue
        if isinstance(v, dict):
            v = v.get("id", v.get("value"))
        try:
            return int(v)
        except (TypeError, ValueError):
            continue
    return None


def _coerce_employee_payload(data: dict[str, Any]) -> dict[str, Any] | None:
    if isinstance(data.get("employee"), dict) and _int_id(data["employee"], "id") is not None:
        return dict(data["employee"])
    if _int_id(data, "id") is not None and (
        "attributes" in data
        or data.get("first_name")
        or data.get("email")
        or data.get("last_name")
    ):
        return data
    return None


def _is_employee_domain_delete(action: str) -> bool:
    a = action.lower()
    if not a.startswith("employee_"):
        return False
    return any(
        x in a
        for x in (
            "delete",
            "destroy",
            "remove",
            "deleted",
        )
    )


def _delete_employee_row(cur: psycopg.Cursor, eid: int) -> None:
    cur.execute(
        "DELETE FROM peopleforce_raw.entity_record WHERE entity_type = %s AND external_id = %s",
        ("employee", str(eid)),
    )
    cur.execute("DELETE FROM peopleforce_dm.employee WHERE id = %s", (eid,))


def _delete_by_table(
    cur: psycopg.Cursor, *, table: str, pk: int, entity_type: str, external_id: str
) -> None:
    cur.execute(
        f"DELETE FROM peopleforce_dm.{table} WHERE id = %s",
        (pk,),
    )
    cur.execute(
        "DELETE FROM peopleforce_raw.entity_record WHERE entity_type = %s AND external_id = %s",
        (entity_type, external_id),
    )


def process_peopleforce_webhook_body(
    action: str,
    data: dict[str, Any],
    cur: psycopg.Cursor,
) -> dict[str, Any]:
    """
    Выполняет SQL через cur; вызывающий обязан commit.
    """
    a = action.lower()

    if a == "leave_request_destroy":
        lid = _int_id(data, "id")
        if lid is None:
            raise HTTPException(
                status_code=400, detail="Missing data.id (leave request)"
            )
        _delete_by_table(
            cur,
            table="leave_request",
            pk=lid,
            entity_type="leave_request",
            external_id=str(lid),
        )
        return {"ok": True, "action": "deleted", "entity": "leave_request", "id": lid}

    if a == "applicant_destroy":
        aid = _int_id(data, "id")
        if aid is None:
            raise HTTPException(status_code=400, detail="Missing data.id (applicant)")
        _delete_by_table(
            cur,
            table="recruitment_candidate",
            pk=aid,
            entity_type="recruitment_candidate",
            external_id=str(aid),
        )
        return {
            "ok": True,
            "action": "deleted",
            "entity": "recruitment_candidate",
            "id": aid,
        }

    if _is_employee_domain_delete(a):
        eid = _int_id(data, "id", "employee_id")
        if eid is None:
            raise HTTPException(
                status_code=400, detail="Missing employee id for delete"
            )
        _delete_employee_row(cur, eid)
        return {"ok": True, "action": "deleted", "entity": "employee", "id": eid}

    if a.startswith("leave_request_"):
        if not upsert_leave_request_row(cur, data):
            raise HTTPException(
                status_code=400, detail="Cannot upsert leave_request (id)"
            )
        lid = int(data["id"])
        upsert_entity_record(
            cur,
            entity_type="leave_request",
            external_id=str(lid),
            raw=data,
        )
        return {"ok": True, "action": "upserted", "entity": "leave_request", "id": lid}

    if a.startswith("applicant_"):
        if not upsert_recruitment_candidate_row(cur, data):
            raise HTTPException(
                status_code=400, detail="Cannot upsert recruitment_candidate (id)"
            )
        aid = int(data["id"])
        upsert_entity_record(
            cur,
            entity_type="recruitment_candidate",
            external_id=str(aid),
            raw=data,
        )
        return {
            "ok": True,
            "action": "upserted",
            "entity": "recruitment_candidate",
            "id": aid,
        }

    if a in (
        "vacancy_application_create",
        "vacancy_application_movement",
        "vacancy_offer_accept",
        "vacancy_offer_reject",
    ):
        if not upsert_recruitment_application_row(cur, data):
            raise HTTPException(
                status_code=400, detail="Cannot upsert recruitment_application (id)"
            )
        apid = int(data["id"])
        upsert_entity_record(
            cur,
            entity_type="recruitment_application",
            external_id=str(apid),
            raw=data,
        )
        return {
            "ok": True,
            "action": "upserted",
            "entity": "recruitment_application",
            "id": apid,
        }

    if a == "vacancy_create":
        if not upsert_recruitment_vacancy_row(cur, data):
            raise HTTPException(
                status_code=400, detail="Cannot upsert recruitment_vacancy (id)"
            )
        vid = int(data["id"])
        upsert_entity_record(
            cur,
            entity_type="recruitment_vacancy",
            external_id=str(vid),
            raw=data,
        )
        return {
            "ok": True,
            "action": "upserted",
            "entity": "recruitment_vacancy",
            "id": vid,
        }

    if a.startswith("employee_"):
        emp = _coerce_employee_payload(data)
        if emp is None:
            eid = _int_id(data, "id", "employee_id")
            if eid is None:
                raise HTTPException(
                    status_code=422,
                    detail="Employee event without full profile: expected data.employee or attributes",
                )
            et = _entity_type_safe(a)
            rid = _int_id(data, "id") or eid
            upsert_entity_record(
                cur,
                entity_type=et,
                external_id=str(rid),
                raw=data,
            )
            return {
                "ok": True,
                "action": "stored_raw",
                "entity": et,
                "id": eid,
                "note": "partial payload; entity_record only",
            }
        merged = (
            merge_webhook_employee_data(emp) if "attributes" in emp else dict(emp)
        )
        eid = _int_id(merged, "id")
        if eid is None:
            raise HTTPException(status_code=400, detail="Missing employee id")
        try:
            flat = flat_employee_row(merged)
        except Exception as ex:
            logger.warning("PeopleForce employee flatten: %s", ex)
            raise HTTPException(
                status_code=422, detail="Unprocessable employee payload"
            ) from ex
        upsert_nested_refs_from_employee(cur, merged)
        upsert_employee_row(cur, flat)
        upsert_entity_record(
            cur,
            entity_type="employee",
            external_id=str(eid),
            raw=merged,
        )
        try:
            link_master_after_pf_employee_upsert(cur, eid, flat)
        except Exception:
            logger.exception("master_link PF employee failed eid=%s", eid)
        return {"ok": True, "action": "upserted", "entity": "employee", "id": eid}

    if a == "workflow" or a.startswith("survey_"):
        rid = _int_id(data, "id")
        et = _entity_type_safe(a)
        if rid is not None:
            ext = str(rid)
        else:
            ext = hashlib.sha256(
                json.dumps(data, sort_keys=True, default=str).encode("utf-8")
            ).hexdigest()[:40]
        upsert_entity_record(
            cur,
            entity_type=et,
            external_id=ext,
            raw=data,
        )
        return {
            "ok": True,
            "action": "stored_raw",
            "entity": et,
            "id": rid,
        }

    return {"ok": True, "skipped": f"unsupported action={action!r}"}


def is_supported_peopleforce_action(action: str) -> bool:
    """True если обрабатываем не skipped (кроме явного skipped в конце диспетчера)."""
    if not action:
        return False
    a = action.lower()
    if a == "leave_request_destroy" or a == "applicant_destroy":
        return True
    if _is_employee_domain_delete(a):
        return True
    if a.startswith(
        (
            "leave_request_",
            "applicant_",
            "employee_",
        )
    ):
        return True
    if a in (
        "vacancy_create",
        "vacancy_application_create",
        "vacancy_application_movement",
        "vacancy_offer_accept",
        "vacancy_offer_reject",
    ):
        return True
    if a == "workflow" or a.startswith("survey_"):
        return True
    return False

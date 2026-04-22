"""
POST /webhook — подписка PeopleForce (Payload URL: https://host/peopleforce/webhook).

Подпись: заголовок x-peopleforce-signature = sha256= + HMAC-SHA256(secret, raw body).
См. https://developer.peopleforce.io/docs/starting-with-webhooks

Типичные action (Topics «сотрудники»): employee_create, employee_update, employee_delete.
Любое событие с подстрокой \"employee\" в action обрабатывается; удаление — если в action
есть delete / destroy / remove / deleted (регистр не важен).
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
from typing import Any

from fastapi import APIRouter, HTTPException, Request

from src.config import get_database_url
from src.db import connect
from src.peopleforce.config import peopleforce_webhook_secret
from src.peopleforce.parse import (
    flat_employee_row,
    merge_webhook_employee_data,
    upsert_employee_row,
    upsert_entity_record,
    upsert_nested_refs_from_employee,
)

logger = logging.getLogger(__name__)

router = APIRouter()


def _verify_signature(raw: bytes, secret: str, header: str | None) -> bool:
    if not secret:
        return True
    if not header or not header.startswith("sha256="):
        return False
    digest = hmac.new(secret.encode("utf-8"), raw, hashlib.sha256).hexdigest()
    expected = f"sha256={digest}"
    return hmac.compare_digest(expected.encode("ascii"), header.encode("ascii"))


def _is_employee_delete(action: str) -> bool:
    a = action.lower()
    if "employee" not in a:
        return False
    return any(x in a for x in ("delete", "destroy", "remove", "deleted"))


def _is_employee_event(action: str) -> bool:
    return "employee" in action.lower()


@router.post("/webhook")
async def peopleforce_webhook(request: Request) -> dict[str, Any]:
    raw = await request.body()
    secret = peopleforce_webhook_secret()
    sig = request.headers.get("x-peopleforce-signature") or request.headers.get(
        "X-Peopleforce-Signature"
    )
    if not _verify_signature(raw, secret, sig):
        raise HTTPException(status_code=401, detail="Invalid signature")

    try:
        body = json.loads(raw.decode("utf-8"))
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="Body must be JSON object")

    action = str(body.get("action") or "")
    data = body.get("data")

    if os.environ.get("PEOPLEFORCE_LOG_BODY", "").strip() in ("1", "true", "yes"):
        logger.info("PeopleForce webhook body (truncated): %s", json.dumps(body)[:8000])

    if not action or not _is_employee_event(action):
        return {"ok": True, "skipped": f"unsupported action={action!r}"}

    if not isinstance(data, dict) or data.get("id") is None:
        raise HTTPException(status_code=400, detail="Missing data.id")

    eid = int(data["id"])

    if _is_employee_delete(action):
        try:
            with connect(get_database_url()) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "DELETE FROM peopleforce_raw.entity_record WHERE entity_type = %s AND external_id = %s",
                        ("employee", str(eid)),
                    )
                    cur.execute(
                        "DELETE FROM peopleforce_dm.employee WHERE id = %s",
                        (eid,),
                    )
                conn.commit()
        except Exception:
            logger.exception("PeopleForce delete failed id=%s", eid)
            raise HTTPException(status_code=500, detail="Internal error") from None
        logger.info("PeopleForce OK delete employee id=%s", eid)
        return {"ok": True, "action": "deleted", "entity": "employee", "id": eid}

    # create / update
    merged = merge_webhook_employee_data(data)
    try:
        flat = flat_employee_row(merged)
    except Exception as e:
        logger.warning("PeopleForce webhook: could not flatten employee: %s", e)
        raise HTTPException(status_code=422, detail="Unprocessable employee payload") from e

    try:
        with connect(get_database_url()) as conn:
            with conn.cursor() as cur:
                upsert_nested_refs_from_employee(cur, merged)
                upsert_employee_row(cur, flat)
                upsert_entity_record(
                    cur,
                    entity_type="employee",
                    external_id=str(eid),
                    raw=merged,
                )
            conn.commit()
    except Exception:
        logger.exception("PeopleForce upsert failed id=%s", eid)
        raise HTTPException(status_code=500, detail="Internal error") from None

    logger.info("PeopleForce OK upsert employee id=%s action=%s", eid, action)
    return {"ok": True, "action": "upserted", "entity": "employee", "id": eid}

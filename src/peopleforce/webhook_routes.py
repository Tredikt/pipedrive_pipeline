"""
POST /webhook — подписка PeopleForce (Payload URL: https://host/peopleforce/webhook).

Подпись: x-peopleforce-signature = sha256= + HMAC-SHA256(secret, raw body).
См. https://developer.peopleforce.io/docs/starting-with-webhooks

Список Topics: https://developer.peopleforce.io/docs/webhooks
(employee_*, leave_request_*, applicant_*, vacancy_*, …)
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
from src.peopleforce.webhook_dispatch import (
    is_supported_peopleforce_action,
    process_peopleforce_webhook_body,
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

    if not action:
        return {"ok": True, "skipped": "empty action"}
    if not is_supported_peopleforce_action(action):
        return {"ok": True, "skipped": f"unsupported action={action!r}"}
    if not isinstance(data, dict):
        raise HTTPException(
            status_code=400, detail="data must be a JSON object for this action"
        )

    try:
        with connect(get_database_url()) as conn:
            with conn.cursor() as cur:
                out = process_peopleforce_webhook_body(action, data, cur)
            conn.commit()
    except HTTPException:
        raise
    except Exception:
        logger.exception("PeopleForce webhook failed action=%s", action)
        raise HTTPException(status_code=500, detail="Internal error") from None

    if out.get("ok"):
        logger.info("PeopleForce webhook OK action=%s out=%s", action, out)
    return out

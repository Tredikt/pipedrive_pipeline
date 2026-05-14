"""POST /webhook с префиксом приложения → URL /jira/webhook (зеркало PeopleForce /peopleforce/webhook)."""


import hashlib
import json
import logging
import os
from typing import Any

from fastapi import APIRouter, Header, HTTPException, Request

from src.config import get_database_url
from src.db import connect
from src.identity_registry import (
    merge_person_identity_from_crm,
    notify_identity_match_miss,
    record_identity_link_pending,
)
from src.webhook_client import parse_ip_allowlist, webhook_client_host

logger = logging.getLogger(__name__)

router = APIRouter()


def _jira_secret() -> str:
    js = os.environ.get("JIRA_WEBHOOK_SECRET", "").strip()
    if js:
        return js
    return os.environ.get("WEBHOOK_SECRET", "").strip()


def _pending_entity_id_jira(account_id: str) -> int:
    digest = hashlib.sha256(f"jira:{account_id}".encode()).digest()
    return int.from_bytes(digest[:8], "big") & 0x7FFFFFFFFFFFFFFF


def _clean_str(x: Any) -> str | None:
    if x is None:
        return None
    s = str(x).strip()
    return s or None


def _tier_key(row: tuple[int, int, Any, Any, Any]) -> tuple[int, int]:
    tier, dual, *_ = row
    return tier, dual


def _walk_best(obj: Any, depth: int) -> tuple[int, int, str | None, str | None, str | None]:
    """
    По всему JSON выбираем «лучший» фрагмент пользователя Atlassian.

    Приоритет: есть accountId (tier=2), затем оба поля (dual=1), затем есть только email (tier=1).
    Так не теряется account_id, если где-то раньше в дереве нашёлся «голый» email.
    """
    if depth > 10 or not isinstance(obj, dict):
        return -1, 0, None, None, None

    aid = _clean_str(obj.get("accountId") or obj.get("account_id"))
    em = _clean_str(obj.get("emailAddress") or obj.get("email"))
    disp = _clean_str(obj.get("displayName") or obj.get("name"))

    tier = 2 if aid is not None else (1 if em is not None else 0)
    dual = 1 if (aid is not None and em is not None) else 0

    candidates: list[tuple[int, int, str | None, str | None, str | None]] = [(tier, dual, aid, em, disp)]
    for v in obj.values():
        candidates.append(_walk_best(v, depth + 1))
    return max(candidates, key=_tier_key)


def _pick_user_fields(body: dict[str, Any]) -> tuple[str | None, str | None, str | None]:
    known_first: list[Any] = []
    for key in ("user", "actor", "modifier", "author", "changelogAuthor"):
        sub = body.get(key)
        if isinstance(sub, dict):
            known_first.append(sub)
    buckets = (*known_first, body)
    best = (-1, 0, None, None, None)
    for root in buckets:
        best = max(best, _walk_best(root, 0), key=_tier_key)
    _, _, aid, email, disp = best
    return aid, email, disp


@router.post("/webhook", summary="Jira webhook")
async def jira_webhook(
    request: Request,
    authorization: str | None = Header(None, alias="Authorization"),
) -> dict[str, Any]:
    secret = _jira_secret()
    if secret:
        expected = f"Bearer {secret}"
        if (authorization or "").strip() != expected:
            raise HTTPException(status_code=401, detail="Unauthorized")

    allow = parse_ip_allowlist(os.environ.get("WEBHOOK_ALLOWED_IPS", ""))
    if allow:
        host = webhook_client_host(request)
        if host not in allow:
            logger.warning("Jira webhook rejected client_host=%s not in allowed list", host)
            raise HTTPException(status_code=403, detail="Forbidden")

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="Body must be JSON object")

    if os.environ.get("JIRA_WEBHOOK_LOG_BODY", "").strip() in ("1", "true", "yes"):
        try:
            logger.info(
                "Jira webhook body (truncated): %s", json.dumps(body, ensure_ascii=False)[:12000]
            )
        except Exception:
            logger.info("Jira webhook body present (json dump failed)")

    aid, email, disp = _pick_user_fields(body)
    if not aid and not email:
        logger.info("Jira webhook skipped: no user fields, keys=%s", list(body.keys()))
        return {"ok": True, "skipped": "no_jira_user_in_payload"}

    if not aid:
        logger.info("Jira webhook skipped: email without accountId (cannot key pending)")
        return {"ok": True, "skipped": "missing_account_id"}

    eff_email = email
    merged = False
    try:
        with connect(get_database_url()) as conn:
            with conn.cursor() as cur:
                merged = merge_person_identity_from_crm(
                    cur,
                    email=eff_email,
                    jira_id=aid,
                    full_name_hint=disp,
                )
                if not merged and eff_email:
                    eid = _pending_entity_id_jira(aid)
                    detail = (
                        "Нет person_identity по этому email "
                        "(строку создаёт PeopleForce). "
                        f"jira_account_id={aid}."
                    )
                    record_identity_link_pending(
                        cur,
                        source_system="jira",
                        entity_kind="user",
                        entity_id=eid,
                        email=eff_email,
                        detail=detail,
                        payload=body,
                    )
                    notify_identity_match_miss(
                        source_system="jira",
                        entity_kind="user",
                        entity_id=eid,
                        email=eff_email,
                        detail=detail,
                    )
                conn.commit()
    except Exception:
        logger.exception("Jira webhook DB error account_id=%s", aid)
        raise HTTPException(status_code=500, detail="Internal error") from None

    if merged:
        logger.info("Jira webhook OK merged jira_id for account_id=%s", aid)
        return {"ok": True, "merged": True, "jira_account_id": aid}

    if not eff_email:
        logger.info(
            "Jira webhook: no merge (missing email); account_id=%s — jira_id not applied",
            aid,
        )
        return {
            "ok": True,
            "merged": False,
            "skipped": "missing_email",
            "jira_account_id": aid,
        }

    logger.info("Jira webhook: recorded pending for account_id=%s", aid)
    return {"ok": True, "merged": False, "pending": True, "jira_account_id": aid}

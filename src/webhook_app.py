"""
HTTP-приёмник webhooks Pipedrive → PostgreSQL.

Запуск локально:
  pip install -r requirements-webhook.txt
  uvicorn src.webhook_app:app --host 0.0.0.0 --port 8000

Переменные: как у синка (DATABASE_URL, PIPEDRIVE_API_TOKEN, PIPEDRIVE_COMPANY_DOMAIN),
опционально WEBHOOK_SECRET — тогда заголовок Authorization: Bearer <WEBHOOK_SECRET>.
"""

from __future__ import annotations

import json
import logging
import os

from fastapi import FastAPI, Header, HTTPException, Request

from src.config import get_settings
from src.db import connect
from src.pipedrive_client import PipedriveClient
from src.sync import sync_one_entity_by_id
from src.webhook_delete import delete_entity_from_db
from src.webhook_parse import is_delete_action, is_upsert_action, parse_webhook_event

logger = logging.getLogger(__name__)

app = FastAPI(title="Pipedrive → PostgreSQL webhooks", version="1.0.0")


@app.on_event("startup")
def _configure_logging() -> None:
    lvl = os.environ.get("LOG_LEVEL", "INFO").strip().upper()
    level = getattr(logging, lvl, logging.INFO)
    logging.getLogger("src.webhook_app").setLevel(level)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/webhook")
async def receive_webhook(
    request: Request,
    authorization: str | None = Header(None, alias="Authorization"),
) -> dict:
    secret = os.environ.get("WEBHOOK_SECRET", "").strip()
    if secret:
        expected = f"Bearer {secret}"
        if (authorization or "").strip() != expected:
            raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="Body must be JSON object")

    if os.environ.get("WEBHOOK_LOG_BODY", "").strip() in ("1", "true", "yes"):
        try:
            raw = json.dumps(body, ensure_ascii=False)[:12000]
            logger.info("Webhook raw body (truncated): %s", raw)
        except Exception:
            logger.info("Webhook body present (json dump failed)")

    parsed = parse_webhook_event(body)
    if parsed is None:
        logger.info(
            "Webhook skipped: unsupported meta, top-level keys=%s",
            list(body.keys()),
        )
        return {"ok": True, "skipped": "unknown_or_unsupported_meta"}

    action, spec_name, entity_id = parsed
    logger.info(
        "Webhook event action=%s spec=%s entity_id=%s",
        action,
        spec_name,
        entity_id,
    )

    try:
        settings = get_settings()
    except RuntimeError as e:
        logger.error("Webhook: misconfiguration (%s)", e)
        raise HTTPException(status_code=503, detail="Service misconfigured") from None
    client = PipedriveClient(
        base_url=settings.pipedrive_api_base_url,
        api_token=settings.pipedrive_api_token,
    )

    if is_delete_action(action):
        try:
            with connect(settings.database_url) as conn:
                delete_entity_from_db(conn, spec_name, entity_id)
                conn.commit()
        except Exception:
            logger.exception(
                "Webhook delete failed spec=%s id=%s", spec_name, entity_id
            )
            raise HTTPException(status_code=500, detail="Internal error") from None
        logger.info("Webhook OK delete spec=%s id=%s", spec_name, entity_id)
        return {"ok": True, "action": "deleted", "spec": spec_name, "id": entity_id}

    if is_upsert_action(action):
        try:
            with connect(settings.database_url) as conn:
                ok = sync_one_entity_by_id(client, conn, spec_name, entity_id)
                conn.commit()
        except Exception:
            logger.exception(
                "Webhook upsert failed spec=%s id=%s", spec_name, entity_id
            )
            raise HTTPException(status_code=500, detail="Internal error") from None
        logger.info(
            "Webhook OK upsert spec=%s id=%s fetch_ok=%s",
            spec_name,
            entity_id,
            ok,
        )
        return {
            "ok": ok,
            "action": "upserted" if ok else "fetch_failed",
            "spec": spec_name,
            "id": entity_id,
        }

    logger.info("Webhook skipped: action=%s (not delete/upsert)", action)
    return {"ok": True, "skipped": f"action={action}"}

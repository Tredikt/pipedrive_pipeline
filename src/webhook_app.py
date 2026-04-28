"""
HTTP-приёмник: Pipedrive POST /webhook, PeopleForce POST /peopleforce/webhook.

Запуск локально:
  pip install -r requirements-webhook.txt
  uvicorn src.webhook_app:app --host 0.0.0.0 --port 8000

Переменные: как у синка (DATABASE_URL, PIPEDRIVE_API_TOKEN, PIPEDRIVE_COMPANY_DOMAIN),
опционально WEBHOOK_SECRET — тогда заголовок Authorization: Bearer <WEBHOOK_SECRET>.
Опционально HR_MATCH_ALERT_WEBHOOK_URL — Incoming Webhook Slack или любой POST JSON:
отправляется объект с полями reason, ids и коротким «text» на русском (инструкция менеджеру).

Правило данных: новые строки master добавляет только PeopleForce (вебхук employee_* может INSERT).
Pipedrive только дописывает идентификаторы к уже существующей строке; при отсутствии совпадения —
уведомление, без создания строки HR из CRM.
Проверка аккаунта Pipedrive: meta.host в теле webhook v2 должен совпадать с доменом из
PIPEDRIVE_COMPANY_DOMAIN или WEBHOOK_EXPECTED_HOST (иначе 403). Опционально WEBHOOK_ALLOWED_IPS.

Логи приложения: handlers в startup, LOG_LEVEL.
"""

from __future__ import annotations

import json
import logging
import os
import sys

from fastapi import FastAPI, Header, HTTPException, Request

from src.config import get_settings
from src.db import connect
from src.master_link import link_master_after_pipedrive_upsert
from src.pipedrive_client import PipedriveClient
from src.sync import sync_one_entity_webhook
from src.webhook_delete import delete_entity_from_db
from src.webhook_parse import is_delete_action, is_upsert_action, parse_webhook_event
from src.peopleforce import webhook_routes as peopleforce_webhook_routes
from src.webhook_client import (
    expected_pipedrive_meta_hostname,
    parse_ip_allowlist,
    webhook_client_host,
    webhook_meta_host_matches_company,
)

logger = logging.getLogger(__name__)

app = FastAPI(
    title="CRM webhooks (Pipedrive + PeopleForce)",
    version="1.0.0",
)
app.include_router(
    peopleforce_webhook_routes.router,
    prefix="/peopleforce",
    tags=["peopleforce"],
)


@app.on_event("startup")
def _configure_logging() -> None:
    lvl = os.environ.get("LOG_LEVEL", "INFO").strip().upper()
    level = getattr(logging, lvl, logging.INFO)
    fmt = logging.Formatter("%(levelname)s %(name)s: %(message)s")
    for name in (
        "src.webhook_app",
        "src.sync",
        "src.pipedrive_client",
        "src.peopleforce.webhook_routes",
    ):
        lg = logging.getLogger(name)
        lg.setLevel(level)
        if not lg.handlers:
            h = logging.StreamHandler(sys.stderr)
            h.setFormatter(fmt)
            lg.addHandler(h)
        lg.propagate = False


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

    allow = parse_ip_allowlist(os.environ.get("WEBHOOK_ALLOWED_IPS", ""))
    if allow:
        host = webhook_client_host(request)
        if host not in allow:
            logger.warning("Webhook rejected client_host=%s not in allowed list", host)
            raise HTTPException(status_code=403, detail="Forbidden")

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="Body must be JSON object")

    if os.environ.get("WEBHOOK_DISABLE_META_HOST_CHECK", "").strip() not in (
        "1",
        "true",
        "yes",
    ):
        if not webhook_meta_host_matches_company(body):
            exp = expected_pipedrive_meta_hostname()
            _meta = body.get("meta") if isinstance(body.get("meta"), dict) else {}
            logger.warning(
                "Webhook rejected: meta.host mismatch or missing (expected=%s got_host=%r)",
                exp,
                _meta.get("host"),
            )
            raise HTTPException(status_code=403, detail="Forbidden")

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
                ok = sync_one_entity_webhook(
                    client, conn, spec_name, entity_id, webhook_body=body
                )
                if ok:
                    try:
                        eid = int(str(entity_id).strip())
                        with conn.cursor() as cur:
                            link_master_after_pipedrive_upsert(cur, spec_name, eid)
                    except (ValueError, TypeError):
                        logger.warning(
                            "master_link: invalid entity_id=%r", entity_id
                        )
                    except Exception:
                        logger.exception(
                            "master_link after Pipedrive webhook failed spec=%s id=%s",
                            spec_name,
                            entity_id,
                        )
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

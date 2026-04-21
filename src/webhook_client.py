"""IP allowlist (опционально) и проверка company host из тела webhook Pipedrive v2 (meta.host)."""

from __future__ import annotations

import os

from fastapi import Request

from src.config import _normalize_company_domain


def webhook_client_host(request: Request) -> str:
    """
    Для сравнения с WEBHOOK_ALLOWED_IPS: реальный клиент за прокси — первый IP
    в X-Forwarded-For (если заголовок есть), иначе адрес TCP-сессии к uvicorn.
    """
    xff = request.headers.get("x-forwarded-for") or request.headers.get("X-Forwarded-For")
    if xff:
        return xff.split(",")[0].strip()
    xri = request.headers.get("x-real-ip") or request.headers.get("X-Real-IP")
    if xri:
        return xri.strip()
    if request.client:
        return request.client.host
    return ""


def parse_ip_allowlist(raw: str) -> frozenset[str]:
    s = raw.strip()
    if not s:
        return frozenset()
    return frozenset(x.strip() for x in s.split(",") if x.strip())


def expected_pipedrive_meta_hostname() -> str | None:
    """
    Ожидаемый meta.host из env: WEBHOOK_EXPECTED_HOST или поддомен из PIPEDRIVE_COMPANY_DOMAIN.
    Формат как у Pipedrive в webhook v2: поддомен.pipedrive.com
    """
    explicit = os.environ.get("WEBHOOK_EXPECTED_HOST", "").strip()
    raw = explicit or os.environ.get("PIPEDRIVE_COMPANY_DOMAIN", "").strip()
    if not raw:
        return None
    sub = _normalize_company_domain(raw)
    if not sub:
        return None
    return f"{sub}.pipedrive.com".lower()


def webhook_meta_host_matches_company(body: dict) -> bool:
    """
    True если проверка не настроена, либо meta.host совпадает с ожидаемым CRM-доменом Pipedrive.
    Webhooks v2: meta.host — компания-отправитель (тот же аккаунт, что в настройках webhook).
    """
    expected = expected_pipedrive_meta_hostname()
    if expected is None:
        return True
    meta = body.get("meta")
    if not isinstance(meta, dict):
        return False
    got = meta.get("host")
    if not isinstance(got, str) or not got.strip():
        return False
    return got.strip().lower() == expected

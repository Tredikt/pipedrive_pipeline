"""
Регистрация webhooks в Pipedrive через POST /v1/webhooks.

Переменные: PIPEDRIVE_API_TOKEN, PIPEDRIVE_COMPANY_DOMAIN,
WEBHOOK_PUBLIC_URL — полный URL, например https://hooks.example.com/webhook

  python scripts/register_pipedrive_webhooks.py --dry-run
  python scripts/register_pipedrive_webhooks.py

Список существующих: GET /v1/webhooks (можно в Postman).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv

load_dotenv()

import httpx

from src.config import resolve_pipedrive_api_base_url


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true", help="Только печать тел запросов")
    p.add_argument(
        "--url",
        default=os.environ.get("WEBHOOK_PUBLIC_URL", "").strip(),
        help="Публичный HTTPS URL эндпоинта (или env WEBHOOK_PUBLIC_URL)",
    )
    args = p.parse_args()

    token = os.environ.get("PIPEDRIVE_API_TOKEN", "").strip()
    if not token:
        raise SystemExit("Нужен PIPEDRIVE_API_TOKEN")
    base = resolve_pipedrive_api_base_url().rstrip("/")
    if not args.url:
        raise SystemExit("Укажите --url или WEBHOOK_PUBLIC_URL")

    subscription_url = args.url.rstrip("/")
    if not subscription_url.startswith("https://"):
        print("Предупреждение: Pipedrive рекомендует HTTPS для subscription_url.")

    # Один webhook на все события (см. лимит 40 webhooks на пользователя в доке Pipedrive).
    body = {
        "name": "postgresql-data-pipeline",
        "subscription_url": subscription_url,
        "event_action": "*",
        "event_object": "*",
        "version": "2.0",
    }

    if args.dry_run:
        print(json.dumps(body, ensure_ascii=False, indent=2))
        return

    r = httpx.post(
        f"{base}/v1/webhooks",
        params={"api_token": token},
        json=body,
        timeout=60.0,
    )
    print(f"HTTP {r.status_code}", flush=True)
    if r.status_code >= 400:
        print(r.text[:800])
        raise SystemExit(1)
    try:
        print(json.dumps(r.json(), ensure_ascii=False, indent=2)[:2000])
    except Exception:
        print(r.text[:500])

    print("Готово. Проверьте в Pipedrive: Настройки → Webhooks — статусы доставки.")


if __name__ == "__main__":
    main()

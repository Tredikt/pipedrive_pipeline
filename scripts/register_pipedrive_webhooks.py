"""
Регистрация webhooks в Pipedrive через POST /v1/webhooks.

Можно несколько webhooks с *.* на разные URL — старый не трогаем, второй:
  python scripts/register_pipedrive_webhooks.py --url https://pulse.omnic.solutions --name omnic-pulse

Переменные: PIPEDRIVE_API_TOKEN, PIPEDRIVE_COMPANY_DOMAIN,
WEBHOOK_PUBLIC_URL — домен или полный URL; если нет пути, добавляется /webhook.

В официальном API есть создание и удаление webhook; URL поменять можно удалением
старого и созданием нового.

  python scripts/register_pipedrive_webhooks.py --dry-run
  python scripts/register_pipedrive_webhooks.py --list
  python scripts/register_pipedrive_webhooks.py --delete-id 1655570
  python scripts/register_pipedrive_webhooks.py

Список: GET /v1/webhooks.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from urllib.parse import urlparse, urlunparse

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv

load_dotenv(_ROOT / ".env")

import httpx

from src.config import resolve_pipedrive_api_base_url


def normalize_subscription_url(raw: str) -> str:
    """Путь по умолчанию /webhook (как у src.webhook_app). Иной путь оставляем как есть."""
    s = raw.strip()
    if not s:
        return ""
    if "://" not in s:
        s = "https://" + s.lstrip("/")
    p = urlparse(s)
    scheme, netloc = (p.scheme or "https"), p.netloc
    if not netloc:
        raise SystemExit(f"Некорректный URL webhook: {raw!r} (нет хоста)")
    path = (p.path or "").strip("/")
    if not path:
        path = "webhook"
    full = "/" + path
    return urlunparse((scheme, netloc, full, "", "", ""))


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true", help="Только печать тел запросов")
    p.add_argument(
        "--url",
        default=os.environ.get("WEBHOOK_PUBLIC_URL", "").strip(),
        help="Публичный HTTPS URL (или env WEBHOOK_PUBLIC_URL)",
    )
    p.add_argument(
        "--delete-id",
        type=int,
        metavar="ID",
        help="Удалить webhook по id (DELETE /v1/webhooks/{id}), затем выход",
    )
    p.add_argument(
        "--list",
        action="store_true",
        help="Показать существующие webhooks (id, subscription_url, event_object.action)",
    )
    p.add_argument(
        "--name",
        default=os.environ.get("WEBHOOK_REGISTER_NAME", "postgresql-data-pipeline").strip()
        or "postgresql-data-pipeline",
        help="Имя webhook в Pipedrive (по умолчанию postgresql-data-pipeline или WEBHOOK_REGISTER_NAME)",
    )
    args = p.parse_args()

    token = os.environ.get("PIPEDRIVE_API_TOKEN", "").strip()
    if not token:
        raise SystemExit("Нужен PIPEDRIVE_API_TOKEN")
    base = resolve_pipedrive_api_base_url().rstrip("/")

    if args.list:
        r = httpx.get(
            f"{base}/v1/webhooks",
            params={"api_token": token},
            timeout=60.0,
        )
        print(f"HTTP {r.status_code}", flush=True)
        if r.status_code >= 400:
            print(r.text[:1200])
            raise SystemExit(1)
        try:
            data = r.json().get("data") or []
            if not data:
                print("(нет webhooks)")
                return
            for wh in data:
                wid = wh.get("id")
                url = wh.get("subscription_url")
                ea = wh.get("event_action")
                eo = wh.get("event_object")
                name = wh.get("name")
                print(f"id={wid}  {eo}.{ea}  {url}  ({name})")
        except Exception:
            print(r.text[:1500])
        return

    if args.delete_id is not None:
        wid = args.delete_id
        if args.dry_run:
            print(json.dumps({"DELETE": f"{base}/v1/webhooks/{wid}"}, indent=2))
            return
        r = httpx.delete(
            f"{base}/v1/webhooks/{wid}",
            params={"api_token": token},
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
        print("Deleted.", flush=True)
        return

    if not args.url:
        raise SystemExit("Укажите --url или WEBHOOK_PUBLIC_URL")

    subscription_url = normalize_subscription_url(args.url)
    if not subscription_url.startswith("https://"):
        print("Предупреждение: Pipedrive рекомендует HTTPS для subscription_url.")

    body = {
        "name": args.name,
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

    print(
        "Done. In Pipedrive: Settings - Webhooks - delivery status.",
        flush=True,
    )


if __name__ == "__main__":
    main()

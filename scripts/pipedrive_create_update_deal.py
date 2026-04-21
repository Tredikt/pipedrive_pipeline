"""
Создать сделку в Pipedrive и сразу изменить название (POST + PUT) — проверка API и цепочки до webhook.

Нужен .env: PIPEDRIVE_API_TOKEN, PIPEDRIVE_COMPANY_DOMAIN (или PIPEDRIVE_API_BASE_URL).

  python scripts/pipedrive_create_update_deal.py
  python scripts/pipedrive_create_update_deal.py --prefix "test from script"

Проверка, что события доходят до вашего URL (webhook уже зарегистрирован в Pipedrive):

1) На VPS с контейнером webhook: docker compose -f docker-compose.webhook.yml logs -f
2) Запустите этот скрипт локально (тот же аккаунт Pipedrive, что и у webhook).
3) В логах должны появиться POST /webhook 200 и строки Webhook event ... spec=deals ...
   (создание и правка — два события; при необходимости WEBHOOK_LOG_BODY=1 в .env контейнера).
4) Дополнительно: Pipedrive — Settings — Webhooks — журнал доставки по subscription_url.

Удалить сделку по API (тот же токен, что и при создании):

  python scripts/pipedrive_create_update_deal.py --delete-id 7321
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv

load_dotenv()

from src.config import get_settings
from src.pipedrive_client import PipedriveClient


DEALS_PATH = "/v1/deals"


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--prefix",
        default="pipedrive_pipeline local",
        help="Префикс названия сделки",
    )
    p.add_argument(
        "--delete-id",
        type=int,
        default=None,
        metavar="ID",
        help="Удалить сделку DELETE /v1/deals/{id} и выйти (без POST/PUT)",
    )
    args = p.parse_args()

    settings = get_settings()
    client = PipedriveClient(
        base_url=settings.pipedrive_api_base_url,
        api_token=settings.pipedrive_api_token,
    )

    if args.delete_id is not None:
        path = f"{DEALS_PATH.rstrip('/')}/{args.delete_id}"
        print("DELETE", path, flush=True)
        resp = client.delete_item(path)
        if not resp.get("success", True):
            print("API error:", resp, flush=True)
            raise SystemExit(1)
        print("OK. Deleted deal id=", args.delete_id, flush=True)
        print("Webhook: expect delete event for spec=deals id=", args.delete_id, flush=True)
        return

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    title_create = f"{args.prefix} [{ts}]"

    print("POST", DEALS_PATH, "title=", title_create, flush=True)
    created = client.post_json(DEALS_PATH, json_body={"title": title_create})
    if not created.get("success", True):
        print("API error:", created, flush=True)
        raise SystemExit(1)
    data = created.get("data")
    if not isinstance(data, dict) or data.get("id") is None:
        print("Unexpected response:", created, flush=True)
        raise SystemExit(1)
    deal_id = data["id"]
    print("Created deal id=", deal_id, flush=True)

    title_edit = f"{title_create} | edited"
    path_put = f"{DEALS_PATH.rstrip('/')}/{deal_id}"
    print("PUT", path_put, flush=True)
    updated = client.put_json(path_put, json_body={"title": title_edit})
    if not updated.get("success", True):
        print("Update error:", updated, flush=True)
        raise SystemExit(1)
    print("OK. New title:", title_edit, flush=True)
    print("Deal id (for DB check):", deal_id, flush=True)
    print("", flush=True)
    print("Webhook verification:", flush=True)
    print("  1) On webhook host: docker compose -f docker-compose.webhook.yml logs -f", flush=True)
    print("  2) Expect two events (create+change) for spec=deals id=" + str(deal_id), flush=True)
    print("  3) Pipedrive: Settings -> Webhooks -> delivery log for your URL", flush=True)


if __name__ == "__main__":
    main()

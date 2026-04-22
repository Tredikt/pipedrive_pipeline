"""
E2E: через API создать сделку → изменить → удалить; опционально проверить ту же PostgreSQL, что использует webhook.

Цепочка webhook → БД:
  - create / change: тело v2 с полем data → upsert в pipedrive_dm + pipedrive_raw.entity_record (без GET, если есть data).
  - delete: удаление из витрины, entity_record, custom_field_value.

Переменные: PIPEDRIVE_*, для --check-db — DATABASE_URL (тот же инстанс, куда пишет поднятый webhook).

  python scripts/e2e_deal_webhook_db.py
  python scripts/e2e_deal_webhook_db.py --check-db --pause 4

Пока идёт пауза, webhook на сервере должен успеть обработать события. Смотрите логи контейнера.
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv

load_dotenv()

from src.config import get_database_url, get_settings
from src.db import connect
from src.pipedrive_client import PipedriveClient

DEALS = "/v1/deals"


def _count_entity_record(conn, deal_id: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) FROM pipedrive_raw.entity_record
            WHERE entity_type = %s AND pipedrive_id = %s
            """,
            ("deals", deal_id),
        )
        row = cur.fetchone()
        return int(row[0]) if row else 0


def _deal_title_dm(conn, deal_id: int) -> str | None:
    with conn.cursor() as cur:
        cur.execute(
            'SELECT title FROM pipedrive_dm.deal WHERE id = %s',
            (deal_id,),
        )
        row = cur.fetchone()
        return str(row[0]) if row and row[0] is not None else None


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--prefix",
        default="e2e webhook",
        help="Префикс названия сделки",
    )
    p.add_argument(
        "--check-db",
        action="store_true",
        help="После каждого шага ждать и проверять PostgreSQL (нужен DATABASE_URL)",
    )
    p.add_argument(
        "--pause",
        type=float,
        default=3.0,
        help="Секунды ожидания webhook после API-шага (при --check-db)",
    )
    args = p.parse_args()

    if args.check_db:
        try:
            get_database_url()
        except RuntimeError as e:
            raise SystemExit(f"--check-db needs DATABASE_URL: {e}") from e

    settings = get_settings()
    client = PipedriveClient(
        base_url=settings.pipedrive_api_base_url,
        api_token=settings.pipedrive_api_token,
    )

    title1 = f"{args.prefix} step1"
    print("1) POST", DEALS, title1, flush=True)
    c = client.post_json(DEALS, json_body={"title": title1})
    if not c.get("success", True):
        raise SystemExit(f"create failed: {c}")
    deal_id = int(c["data"]["id"])
    print("   deal id =", deal_id, flush=True)

    if args.check_db:
        time.sleep(args.pause)
        with connect(get_database_url()) as conn:
            er = _count_entity_record(conn, str(deal_id))
            t = _deal_title_dm(conn, deal_id)
            print(
                f"   DB after create: entity_record rows={er}, dm.deal.title={t!r}",
                flush=True,
            )

    title2 = f"{args.prefix} step2 edited"
    path = f"{DEALS.rstrip('/')}/{deal_id}"
    print("2) PUT", path, title2, flush=True)
    u = client.put_json(path, json_body={"title": title2})
    if not u.get("success", True):
        raise SystemExit(f"update failed: {u}")

    if args.check_db:
        time.sleep(args.pause)
        with connect(get_database_url()) as conn:
            t = _deal_title_dm(conn, deal_id)
            print(
                f"   DB after change: dm.deal.title={t!r} (expect step2)",
                flush=True,
            )

    print("3) DELETE", path, flush=True)
    d = client.delete_item(path)
    if not d.get("success", True):
        raise SystemExit(f"delete failed: {d}")

    if args.check_db:
        time.sleep(args.pause)
        with connect(get_database_url()) as conn:
            er = _count_entity_record(conn, str(deal_id))
            t_exists = _deal_title_dm(conn, deal_id)
            print(
                f"   DB after delete: entity_record rows={er}, dm.deal exists={t_exists is not None}",
                flush=True,
            )
        print("   Expect: entity_record=0, dm row gone (if webhook delete processed).", flush=True)

    print("Done. On webhook host check logs: create, change, delete for spec=deals id=", deal_id, flush=True)


if __name__ == "__main__":
    main()

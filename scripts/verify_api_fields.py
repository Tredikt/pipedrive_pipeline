"""
Проверка: подтянуть *Fields напрямую из Pipedrive API и вывести имена полей.

Нужны в .env: PIPEDRIVE_API_TOKEN, при необходимости PIPEDRIVE_COMPANY_DOMAIN.
DATABASE_URL не используется.

  python scripts/verify_api_fields.py
  python scripts/verify_api_fields.py --show 200
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import os

from dotenv import load_dotenv

load_dotenv()

from src.config import resolve_pipedrive_api_base_url
from src.pipedrive_client import PipedriveClient, pipedrive_list_next_start


def _field_rows(body: dict) -> list[dict]:
    data = body.get("data")
    if data is None:
        return []
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        return [v for v in data.values() if isinstance(v, dict)]
    return []


def _load_all_fields(client: PipedriveClient, path: str) -> list[dict]:
    rows: list[dict] = []
    start = 0
    limit = 500
    while True:
        body = client.get_json(path, params={"start": start, "limit": limit})
        if not body.get("success", True):
            raise RuntimeError(f"{path}: {body}")
        batch = _field_rows(body)
        rows.extend(batch)
        nxt = pipedrive_list_next_start(
            body, start=start, page_size=limit, row_count=len(batch)
        )
        if nxt is None:
            break
        start = nxt
    return rows


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--show", type=int, default=50, help="Сколько полей печатать на сущность (0 = только счётчик)")
    args = p.parse_args()

    token = os.environ.get("PIPEDRIVE_API_TOKEN", "").strip()
    if not token:
        raise SystemExit("Задайте PIPEDRIVE_API_TOKEN в .env")

    base = resolve_pipedrive_api_base_url()
    client = PipedriveClient(base_url=base, api_token=token)

    endpoints: list[tuple[str, str]] = [
        ("/v1/dealFields", "deals"),
        ("/v1/leadFields", "leads"),
        ("/v1/personFields", "persons"),
        ("/v1/organizationFields", "organizations"),
        ("/v1/productFields", "products"),
        ("/v1/activityFields", "activities"),
        ("/v1/noteFields", "notes"),
        ("/v1/projectFields", "projects"),
    ]

    print("Base URL:", base)
    print()

    for path, logical in endpoints:
        try:
            rows = _load_all_fields(client, path)
            names = sorted(
                (str(r.get("name") or ""), str(r.get("key") or ""))
                for r in rows
                if r.get("key")
            )
            print("=" * 72)
            print(f"{logical:14}  {path}  →  {len(names)} полей")
            show = args.show
            if show > 0:
                for name, key in names[:show]:
                    print(f"  {name!r}  (key …{key[-8:]})")
                if len(names) > show:
                    print(f"  … ещё {len(names) - show} полей (увеличьте --show)")
        except Exception as e:
            print("=" * 72)
            print(f"{logical:14}  {path}  →  ОШИБКА: {e}")
        print()


if __name__ == "__main__":
    main()

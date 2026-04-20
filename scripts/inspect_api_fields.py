"""
Печать структуры ответов Pipedrive API: ключи корня JSON и ключи первой записи в data,
или полный JSON ответа (--raw).

Примеры:
  python scripts/inspect_api_fields.py --entity persons
  python scripts/inspect_api_fields.py --entity persons --raw
  python scripts/inspect_api_fields.py --path /v1/persons --raw --limit 1
  python scripts/inspect_api_fields.py --entity deals --limit 3
  python scripts/inspect_api_fields.py --all
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.config import get_settings
from src.entities import ENTITY_SPECS, EntitySpec
from src.pipedrive_client import PipedriveClient


def _first_items(data: Any, n: int) -> list[dict[str, Any]]:
    if data is None:
        return []
    if isinstance(data, list):
        out = [x for x in data if isinstance(x, dict)]
        return out[:n]
    if isinstance(data, dict):
        out = [v for v in data.values() if isinstance(v, dict)]
        return out[:n]
    return []


def _list_limit(spec: EntitySpec) -> int:
    if spec.page_size is not None:
        return min(spec.page_size, 500)
    return 500


def inspect_spec(client: PipedriveClient, spec: EntitySpec, *, sample_limit: int) -> None:
    req_limit = max(1, min(sample_limit, _list_limit(spec)))
    body = client.get_json(spec.list_path, params={"start": 0, "limit": req_limit})
    root_keys = sorted(body.keys())
    data = body.get("data")
    items = _first_items(data, 1)
    first_keys = sorted(items[0].keys()) if items else []

    print(f"\n=== {spec.name}  {spec.list_path}  (request limit={req_limit}) ===")
    print("Корень ответа:", ", ".join(root_keys))
    if items:
        print(f"Ключи первой записи data ({len(first_keys)}):", ", ".join(first_keys))
        if sample_limit > 1 and len(_first_items(data, sample_limit)) > 1:
            union: set[str] = set()
            for row in _first_items(data, sample_limit):
                union |= row.keys()
            print(f"Объединение ключей по первым {sample_limit} записям ({len(union)}):", ", ".join(sorted(union)))
    else:
        print("Первая запись: (нет — data пустой или не объект/список объектов)")
        if data is not None:
            preview = json.dumps(data, ensure_ascii=False, default=str)
            if len(preview) > 400:
                preview = preview[:400] + "…"
            print("data (фрагмент):", preview)

    add = body.get("additional_data")
    if isinstance(add, dict) and add:
        print("additional_data ключи:", ", ".join(sorted(add.keys())))


def _dump_raw_body(body: Any) -> None:
    print(json.dumps(body, ensure_ascii=False, indent=2, default=str))


def main() -> None:
    p = argparse.ArgumentParser(description="Поля (ключи) или полный JSON ответов Pipedrive API")
    p.add_argument("--entity", action="append", dest="entities", help="Имя сущности из ENTITY_SPECS (можно несколько раз)")
    p.add_argument("--all", action="store_true", help="Все сущности из ENTITY_SPECS (много запросов)")
    p.add_argument(
        "--path",
        default=None,
        metavar="PATH",
        help="Произвольный путь, напр. /v1/persons (удобно с --raw)",
    )
    p.add_argument(
        "--raw",
        action="store_true",
        help="Вывести весь JSON ответа (одна --entity, или --path; не сочетать с --all)",
    )
    p.add_argument(
        "--limit",
        type=int,
        default=1,
        metavar="N",
        help="Сколько записей запросить из списка (для объединения ключей по нескольким строкам)",
    )
    args = p.parse_args()

    if args.path and (args.all or args.entities):
        p.error("Не используйте --path вместе с --entity или --all")

    if not args.all and not args.entities and not args.path:
        names = ", ".join(s.name for s in ENTITY_SPECS)
        p.error(f"Укажите --entity <name>, --all или --path. Доступные имена: {names}")

    if args.raw and args.all:
        p.error("С --raw укажите одну сущность (--entity persons) или --path /v1/...")

    if args.raw and args.entities and len(args.entities) != 1:
        p.error("С --raw укажите ровно одну сущность, например: --entity persons")

    settings = get_settings()
    client = PipedriveClient(base_url=settings.pipedrive_api_base_url, api_token=settings.pipedrive_api_token)

    if args.path:
        lim = max(1, args.limit)
        body = client.get_json(args.path if args.path.startswith("/") else f"/{args.path}", params={"start": 0, "limit": lim})
        _dump_raw_body(body)
        return

    if args.all:
        specs = ENTITY_SPECS
    else:
        want = set(args.entities or [])
        specs = tuple(s for s in ENTITY_SPECS if s.name in want)
        missing = want - {s.name for s in specs}
        if missing:
            raise SystemExit(f"Неизвестные сущности: {sorted(missing)}")

    if args.raw:
        spec = specs[0]
        req_limit = max(1, min(args.limit, _list_limit(spec)))
        body = client.get_json(spec.list_path, params={"start": 0, "limit": req_limit})
        _dump_raw_body(body)
        return

    for spec in specs:
        inspect_spec(client, spec, sample_limit=max(1, args.limit))


if __name__ == "__main__":
    main()

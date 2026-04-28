"""
Сырая пагинированная выгрузка всех зарегистрированных GET-листов API v3 в
peopleforce_raw.entity_record (по одной JSON-строке на объект).

Пагинация по страницам — в PeopleForceClient.iter_paginated (metadata.pagination.pages).

403/404 на отдельных путях пропускаются (лог + следующий path).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import sys
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv

load_dotenv()

from src.config import get_database_url
from src.db import connect
from src.peopleforce.bulk_endpoints import (
    RAW_LIST_ENDPOINTS,
    RawListEndpoint,
    default_entity_type,
)
from src.peopleforce.client import PeopleForceClient
from src.peopleforce.config import peopleforce_api_base_url, peopleforce_api_key
from src.peopleforce.parse import upsert_entity_record

log = logging.getLogger(__name__)


def external_id_for_row(row: dict[str, Any]) -> str:
    for key in ("id", "uuid"):
        v = row.get(key)
        if v is not None and v != "":
            return str(v)
    h = hashlib.sha256(
        json.dumps(row, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()
    return f"sha256:{h[:40]}"


def _entity_type(spec: RawListEndpoint) -> str:
    if spec.entity_type:
        return spec.entity_type
    return default_entity_type(spec.path)


def run_raw_bulk(
    *,
    skip_paths: frozenset[str] | None = None,
    only_paths: frozenset[str] | None = None,
    commit_every: int = 200,
) -> dict[str, int]:
    """
    Возвращает словарь path -> количество записанных строк.
    """
    skip = skip_paths or frozenset()
    base = peopleforce_api_base_url()
    key = peopleforce_api_key()
    client = PeopleForceClient(base_url=base, api_key=key)
    counts: dict[str, int] = {}

    with connect(get_database_url()) as conn:
        for spec in RAW_LIST_ENDPOINTS:
            path = spec.path
            if path in skip:
                log.info("skip (listed): %s", path)
                continue
            if only_paths is not None and path not in only_paths:
                continue
            ent = _entity_type(spec)
            n = 0
            with conn.cursor() as cur:
                for row in client.iter_paginated(
                    path,
                    extra_params=spec.extra_params,
                    skip_on_client_error=True,
                ):
                    eid = external_id_for_row(row)
                    upsert_entity_record(
                        cur, entity_type=ent, external_id=eid, raw=row
                    )
                    n += 1
                    if n % commit_every == 0:
                        conn.commit()
                        log.debug("%s: %d rows", path, n)
            conn.commit()
            counts[path] = n
            if n == 0:
                log.warning(
                    "0 rows or path unavailable (403/404/empty): %s " "(entity_type=%s)",
                    path,
                    ent,
                )
            else:
                log.info("OK %s: %d rows (entity_type=%s)", path, n, ent)

    return counts


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(message)s", stream=sys.stderr
    )
    p = argparse.ArgumentParser(
        description="PeopleForce: сырая выгрузка всех list-endpoints в entity_record"
    )
    p.add_argument(
        "--only",
        action="append",
        dest="only_paths",
        default=None,
        help="Повторить флаг для ограничения путями, например --only /teams",
    )
    args = p.parse_args()
    only: frozenset[str] | None = None
    if args.only_paths:
        only = frozenset(args.only_paths)
    counts = run_raw_bulk(only_paths=only)
    total = sum(counts.values())
    log.info("total rows: %d across %d paths", total, len(counts))


if __name__ == "__main__":
    main()

"""Последние активности Pipedrive — только таблица pipedrive_dm.activity.

Строки по synced_at (удобно проверять webhook). Колонки add_time / update_time —
время в CRM.

  python scripts/show_recent_pipedrive_sync.py
  python scripts/show_recent_pipedrive_sync.py --limit 40

Опции: --env-file (по умолчанию корневой .env).
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parents[1]


def _trunc(s: str | None, mx: int) -> str:
    if not s:
        return ""
    t = s.replace("\n", " ")
    return t if len(t) <= mx else t[: mx - 3] + "..."


def main() -> None:
    ap = argparse.ArgumentParser(description="Последние строки pipedrive_dm.activity.")
    ap.add_argument("--env-file", type=Path, default=_ROOT / ".env")
    ap.add_argument("--limit", type=int, default=25)
    args = ap.parse_args()

    load_dotenv(args.env_file)
    url = os.environ.get("DATABASE_URL", "").strip()
    if not url:
        raise SystemExit("DATABASE_URL не задан.")

    try:
        import psycopg
    except ImportError as e:
        raise SystemExit("Нужен psycopg: pip install 'psycopg[binary]'") from e

    lim = max(1, min(args.limit, 500))

    with psycopg.connect(url, connect_timeout=15) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = 'pipedrive_dm'
                      AND table_name = 'activity'
                )
                """
            )
            if cur.fetchone()[0] is not True:
                raise SystemExit(
                    "Нет pipedrive_dm.activity — сначала python -m src.sync --init-db "
                    "и выгрузка активностей."
                )

            cur.execute(
                "SELECT COUNT(*)::bigint, MAX(synced_at) FROM pipedrive_dm.activity"
            )
            total, max_sync = cur.fetchone()

            cur.execute(
                """
                SELECT id, type, subject, done, deal_id, person_id, org_id,
                       add_time, update_time, synced_at
                FROM pipedrive_dm.activity
                ORDER BY synced_at DESC NULLS LAST
                LIMIT %s
                """,
                (lim,),
            )
            rows = cur.fetchall()

    print("pipedrive_dm.activity")
    print(f"  всего строк: {total}  max(synced_at): {max_sync}")
    print()
    hdr = (
        "synced_at                  id     type          done  deal_id  "
        "person_id  org_id   add_time (CRM)      update_time (CRM)"
    )
    print(hdr)
    print("-" * len(hdr))
    for (
        aid,
        atype,
        subj,
        done,
        deal_id,
        person_id,
        org_id,
        add_t,
        upd_t,
        sync_t,
    ) in rows:
        print(
            f"{str(sync_t):<26} {aid:<6} {(atype or ''):<13} {str(done):<5} "
            f"{str(deal_id or ''):<8} {str(person_id or ''):<10} "
            f"{str(org_id or ''):<8} {str(add_t):<19} {str(upd_t)}"
        )
        print(f"  {_trunc(subj, 100)}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _stream_rows(cur, writer: csv.writer) -> int:
    cols = [d.name for d in cur.description]
    writer.writerow(cols)
    n = 0
    while True:
        batch = cur.fetchmany(5000)
        if not batch:
            break
        writer.writerows(batch)
        n += len(batch)
    return n


def main() -> None:
    ap = argparse.ArgumentParser(description="CSV: pipedrive_dm.deal, без lost")
    ap.add_argument(
        "-o",
        "--output",
        default="-",
        help="Файл вывода или - для stdout",
    )
    ap.add_argument(
        "--env-file",
        type=Path,
        default=_ROOT / ".env",
        help="Файл окружения с DATABASE_URL",
    )
    ap.add_argument(
        "--excel",
        action="store_true",
        help="UTF-8 с BOM (удобнее открывать в Excel на Windows)",
    )
    args = ap.parse_args()

    load_dotenv(args.env_file)

    try:
        import psycopg
    except ImportError as e:
        raise SystemExit("Нужен psycopg: pip install 'psycopg[binary]'") from e

    from src.config import get_database_url

    db_url = get_database_url()

    sql = """
        SELECT
            id,
            title,
            value,
            currency,
            stage_id,
            pipeline_id,
            person_id,
            org_id,
            status,
            probability,
            owner_user_id,
            visible_to,
            lost_reason,
            expected_close_date,
            add_time,
            update_time,
            synced_at
        FROM pipedrive_dm.deal
        WHERE status IS NULL OR lower(trim(status)) <> 'lost'
        ORDER BY id
    """

    out_path = args.output.strip()
    if args.excel and out_path == "-":
        print(
            "WARN: --excel игнорируется для stdout; укажите -o файл.",
            file=sys.stderr,
        )

    with psycopg.connect(db_url, connect_timeout=60) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT EXISTS (
                  SELECT 1 FROM information_schema.tables
                  WHERE table_schema = 'pipedrive_dm'
                    AND table_name = 'deal'
                )
                """
            )
            if cur.fetchone()[0] is not True:
                raise SystemExit(
                    "Нет таблицы pipedrive_dm.deal — сначала синхронизируйте Pipedrive."
                )

            cur.execute(sql)

            if out_path == "-":
                n = _stream_rows(cur, csv.writer(sys.stdout, lineterminator="\n"))
            else:
                encoding = "utf-8-sig" if args.excel else "utf-8"
                with open(
                    out_path, "w", newline="", encoding=encoding
                ) as f:
                    n = _stream_rows(cur, csv.writer(f))
                print(f"Записано строк: {n}", file=sys.stderr)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)

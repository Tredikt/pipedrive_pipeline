"""
Инкрементальное обновление GA4 в Postgres (google_analytics_dm).

  pip install -r requirements-google-analytics.txt

  python -m src.google_analytics.sync --init-db
  python -m src.google_analytics.sync --full-history   # с GA_SYNC_START_DATE … today
  python -m src.google_analytics.sync --days 7         # скользящее окно (cron)

GA не отдаёт сырые хиты через Data API — только агрегаты по измерениям;
«за всё время» = один большой диапазон дат + постраничная выборка (offset).

Docker: сервис cron; см. docker/cron.d/*.cron

По умолчанию daily_user не заполняется: в Data API нет измерений userPseudoId/userId.
Нужен user-scoped custom dimension в GA4 и переменная GA_SYNC_USER_CUSTOM_DIMENSION
(или GA_SYNC_USER_DIMENSION=customUser:имя). Отключить явно: none.
"""
from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Iterator
from datetime import date, datetime
from pathlib import Path

from dotenv import load_dotenv

_REPO_ROOT = Path(__file__).resolve().parents[2]

load_dotenv(_REPO_ROOT / ".env")

_SQL_FILES = [
    _REPO_ROOT / "sql" / "022_google_analytics_dm.sql",
    _REPO_ROOT / "sql" / "023_google_analytics_dm_upgrade.sql",
]


def _truthy_env(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in ("1", "true", "yes", "on")


def _resolve_ga_user_dimension(raw: str | None) -> str:
    """
    none | off | - → не грузить daily_user.
    userId / userPseudoId → пусто + предупреждение (в RunReport не поддерживаются).
    """
    s = (raw or "").strip()
    if s.lower() in ("none", "off", "-"):
        return ""
    low = s.replace("_", "").lower()
    if low in ("userid", "userpseudoid"):
        print(
            "WARN: GA Data API не поддерживает измерения userId и userPseudoId в RunReport. "
            "Создайте в GA4 user-scoped custom dimension и укажите "
            "GA_SYNC_USER_CUSTOM_DIMENSION=<параметр> (будет customUser:<параметр>) "
            "или GA_SYNC_USER_DIMENSION=customUser:<параметр>.\n"
            "Схема API: https://developers.google.com/analytics/devguides/reporting/data/v1/api-schema",
            file=sys.stderr,
        )
        return ""
    if not s:
        return ""
    return s


def _raw_user_dimension_from_config(args: argparse.Namespace) -> str | None:
    if args.user_dimension is not None:
        return args.user_dimension
    env_ud = os.environ.get("GA_SYNC_USER_DIMENSION")
    if env_ud is not None:
        return env_ud
    param = os.environ.get("GA_SYNC_USER_CUSTOM_DIMENSION", "").strip()
    if not param:
        return None
    if param.startswith("customUser:"):
        return param
    return f"customUser:{param}"


def _parse_ga_date(s: str) -> date:
    raw = (s or "").strip()
    if len(raw) == 8 and raw.isdigit():
        return date(int(raw[:4]), int(raw[4:6]), int(raw[6:8]))
    return datetime.strptime(raw[:10], "%Y-%m-%d").date()


def _property_resource(raw: str) -> str:
    s = raw.strip()
    if s.startswith("properties/"):
        pid = s.replace("properties/", "", 1).strip("/")
        return f"properties/{pid}"
    return f"properties/{s}"


def _safe_int(val: str | None) -> int | None:
    if val is None or val == "":
        return None
    try:
        return int(float(val))
    except (TypeError, ValueError):
        return None


def _resolve_date_range(
    *,
    full_history: bool,
    start_date_cli: str | None,
    days: int,
) -> dict[str, str]:
    if start_date_cli:
        return {"start_date": start_date_cli.strip(), "end_date": "today"}
    if full_history or _truthy_env("GA_SYNC_FULL_HISTORY"):
        start = os.environ.get("GA_SYNC_START_DATE", "2018-01-01").strip()
        return {"start_date": start, "end_date": "today"}
    d = max(1, min(days, 366))
    return {"start_date": f"{d}daysAgo", "end_date": "today"}


def _range_label(dr: dict[str, str]) -> str:
    return f"{dr['start_date']}->{dr['end_date']}"


def _iter_report_rows(
    client,
    *,
    property_: str,
    dimensions: list[dict[str, str]],
    metrics: list[dict[str, str]],
    date_range: dict[str, str],
    page_size: int,
) -> Iterator:
    from google.analytics.data_v1beta.types import RunReportRequest

    offset = 0
    while True:
        req = RunReportRequest(
            property=property_,
            dimensions=dimensions,
            metrics=metrics,
            date_ranges=[date_range],
            limit=page_size,
            offset=offset,
        )
        resp = client.run_report(req)
        rows = list(resp.rows)
        if not rows:
            break
        yield from rows
        if len(rows) < page_size:
            break
        offset += page_size


def main() -> None:
    ap = argparse.ArgumentParser(description="GA4 Data API → google_analytics_dm")
    ap.add_argument(
        "--init-db",
        action="store_true",
        help="Применить sql/022 и миграцию 023",
    )
    ap.add_argument(
        "--days",
        type=int,
        default=int(os.environ.get("GA_SYNC_DAYS", "7")),
        help="Скользящее окно: NdaysAgo … today (если не --full-history и не --start-date)",
    )
    ap.add_argument(
        "--full-history",
        action="store_true",
        help="С GA_SYNC_START_DATE (по умолчанию 2018-01-01) до today",
    )
    ap.add_argument(
        "--start-date",
        default=None,
        help="Фиксированная нижняя граница YYYY-MM-DD (перекрывает full/sliding)",
    )
    ap.add_argument(
        "--user-dimension",
        default=None,
        metavar="NAME",
        help="Имя измерения GA4 для daily_user, напр. customUser:my_param (по умолчанию см. GA_SYNC_USER_CUSTOM_DIMENSION)",
    )
    ap.add_argument(
        "--page-limit",
        type=int,
        default=int(os.environ.get("GA_SYNC_PAGE_LIMIT", "100000")),
        help="Размер страницы GA API (offset pagination), до 250000",
    )
    args = ap.parse_args()

    _ud_raw = _raw_user_dimension_from_config(args)
    user_dimension = _resolve_ga_user_dimension(_ud_raw)

    from src.config import get_database_url
    from src.db import connect, init_schema

    db_url = get_database_url()

    if args.init_db:
        missing = [p for p in _SQL_FILES if not p.is_file()]
        if missing:
            raise SystemExit(f"Нет файлов: {missing}")
        with connect(db_url) as conn:
            for path in _SQL_FILES:
                init_schema(conn, str(path))
                print(f"Applied {path.name}")
        return

    try:
        from google.analytics.data_v1beta import BetaAnalyticsDataClient
    except ImportError as e:
        raise SystemExit(
            "Установите зависимости GA:\n"
            "  pip install -r requirements-google-analytics.txt"
        ) from e

    from src.google_analytics.auth import load_ga_oauth_credentials

    raw_prop = os.environ.get("GA_PROPERTY", "").strip()
    if not raw_prop:
        raise SystemExit("Задайте GA_PROPERTY в .env")

    prop_res = _property_resource(raw_prop)
    date_range = _resolve_date_range(
        full_history=args.full_history,
        start_date_cli=args.start_date,
        days=args.days,
    )
    page_size = max(1000, min(args.page_limit, 250_000))

    creds = load_ga_oauth_credentials()
    client = BetaAnalyticsDataClient(credentials=creds)

    overview_rows: list[tuple] = []
    for row in _iter_report_rows(
        client,
        property_=prop_res,
        dimensions=[{"name": "date"}],
        metrics=[
            {"name": "activeUsers"},
            {"name": "sessions"},
            {"name": "screenPageViews"},
            {"name": "newUsers"},
        ],
        date_range=date_range,
        page_size=page_size,
    ):
        rd = _parse_ga_date(row.dimension_values[0].value)
        au, ses, spv, nu = [_safe_int(mv.value) for mv in row.metric_values]
        overview_rows.append((rd, au, ses, spv, nu))

    channel_rows: list[tuple] = []
    for row in _iter_report_rows(
        client,
        property_=prop_res,
        dimensions=[
            {"name": "date"},
            {"name": "sessionDefaultChannelGrouping"},
        ],
        metrics=[
            {"name": "sessions"},
            {"name": "activeUsers"},
            {"name": "screenPageViews"},
        ],
        date_range=date_range,
        page_size=page_size,
    ):
        rd = _parse_ga_date(row.dimension_values[0].value)
        ch = row.dimension_values[1].value or ""
        ses, au, spv = [_safe_int(mv.value) for mv in row.metric_values]
        channel_rows.append((rd, ch, ses, au, spv))

    geo_rows: list[tuple] = []
    for row in _iter_report_rows(
        client,
        property_=prop_res,
        dimensions=[{"name": "date"}, {"name": "country"}],
        metrics=[
            {"name": "sessions"},
            {"name": "activeUsers"},
            {"name": "screenPageViews"},
        ],
        date_range=date_range,
        page_size=page_size,
    ):
        rd = _parse_ga_date(row.dimension_values[0].value)
        ctry = row.dimension_values[1].value or ""
        ses, au, spv = [_safe_int(mv.value) for mv in row.metric_values]
        geo_rows.append((rd, ctry, ses, au, spv))

    device_rows: list[tuple] = []
    for row in _iter_report_rows(
        client,
        property_=prop_res,
        dimensions=[{"name": "date"}, {"name": "deviceCategory"}],
        metrics=[
            {"name": "sessions"},
            {"name": "activeUsers"},
            {"name": "screenPageViews"},
        ],
        date_range=date_range,
        page_size=page_size,
    ):
        rd = _parse_ga_date(row.dimension_values[0].value)
        dev = row.dimension_values[1].value or ""
        ses, au, spv = [_safe_int(mv.value) for mv in row.metric_values]
        device_rows.append((rd, dev, ses, au, spv))

    user_rows: list[tuple] = []
    ud = user_dimension
    if ud:
        try:
            for row in _iter_report_rows(
                client,
                property_=prop_res,
                dimensions=[{"name": "date"}, {"name": ud}],
                metrics=[
                    {"name": "sessions"},
                    {"name": "screenPageViews"},
                    {"name": "activeUsers"},
                ],
                date_range=date_range,
                page_size=page_size,
            ):
                rd = _parse_ga_date(row.dimension_values[0].value)
                uid = (row.dimension_values[1].value or "").strip()
                if not uid:
                    continue
                ses, spv, au = [_safe_int(mv.value) for mv in row.metric_values]
                user_rows.append((rd, uid, ses, spv, au))
        except Exception as e:
            print(f"WARN: user dimension {ud!r} skipped: {e}", file=sys.stderr)

    batch_every = 1500

    def flush_sm(cur, batch: list[tuple]) -> None:
        for tup in batch:
            cur.execute(
                """
                INSERT INTO google_analytics_dm.daily_source_medium (
                    report_date, session_source, session_medium, sessions,
                    active_users, screen_page_views, synced_at
                ) VALUES (%s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (report_date, session_source, session_medium)
                DO UPDATE SET
                    sessions = EXCLUDED.sessions,
                    active_users = EXCLUDED.active_users,
                    screen_page_views = EXCLUDED.screen_page_views,
                    synced_at = NOW()
                """,
                tup,
            )

    def flush_pages(cur, batch: list[tuple]) -> None:
        for tup in batch:
            cur.execute(
                """
                INSERT INTO google_analytics_dm.daily_page (
                    report_date, page_path, page_title,
                    screen_page_views, sessions, synced_at
                ) VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (report_date, page_path) DO UPDATE SET
                    page_title = EXCLUDED.page_title,
                    screen_page_views = EXCLUDED.screen_page_views,
                    sessions = EXCLUDED.sessions,
                    synced_at = NOW()
                """,
                tup,
            )

    sm_total = 0
    page_total = 0

    with connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT EXISTS (
                  SELECT 1 FROM information_schema.tables
                  WHERE table_schema = 'google_analytics_dm'
                    AND table_name = 'daily_overview'
                )
                """
            )
            if cur.fetchone()[0] is not True:
                raise SystemExit(
                    "Нет схемы google_analytics_dm — сначала:\n"
                    "  python -m src.google_analytics.sync --init-db"
                )

            for tup in overview_rows:
                cur.execute(
                    """
                    INSERT INTO google_analytics_dm.daily_overview (
                        report_date, active_users, sessions,
                        screen_page_views, new_users, synced_at
                    ) VALUES (%s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (report_date) DO UPDATE SET
                        active_users = EXCLUDED.active_users,
                        sessions = EXCLUDED.sessions,
                        screen_page_views = EXCLUDED.screen_page_views,
                        new_users = EXCLUDED.new_users,
                        synced_at = NOW()
                    """,
                    tup,
                )

            for tup in channel_rows:
                cur.execute(
                    """
                    INSERT INTO google_analytics_dm.daily_channel (
                        report_date, channel, sessions,
                        active_users, screen_page_views, synced_at
                    ) VALUES (%s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (report_date, channel) DO UPDATE SET
                        sessions = EXCLUDED.sessions,
                        active_users = EXCLUDED.active_users,
                        screen_page_views = EXCLUDED.screen_page_views,
                        synced_at = NOW()
                    """,
                    tup,
                )

            for tup in geo_rows:
                cur.execute(
                    """
                    INSERT INTO google_analytics_dm.daily_geo (
                        report_date, country, sessions,
                        active_users, screen_page_views, synced_at
                    ) VALUES (%s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (report_date, country) DO UPDATE SET
                        sessions = EXCLUDED.sessions,
                        active_users = EXCLUDED.active_users,
                        screen_page_views = EXCLUDED.screen_page_views,
                        synced_at = NOW()
                    """,
                    tup,
                )

            for tup in device_rows:
                cur.execute(
                    """
                    INSERT INTO google_analytics_dm.daily_device (
                        report_date, device_category, sessions,
                        active_users, screen_page_views, synced_at
                    ) VALUES (%s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (report_date, device_category) DO UPDATE SET
                        sessions = EXCLUDED.sessions,
                        active_users = EXCLUDED.active_users,
                        screen_page_views = EXCLUDED.screen_page_views,
                        synced_at = NOW()
                    """,
                    tup,
                )

            sm_batch: list[tuple] = []
            for row in _iter_report_rows(
                client,
                property_=prop_res,
                dimensions=[
                    {"name": "date"},
                    {"name": "sessionSource"},
                    {"name": "sessionMedium"},
                ],
                metrics=[
                    {"name": "sessions"},
                    {"name": "activeUsers"},
                    {"name": "screenPageViews"},
                ],
                date_range=date_range,
                page_size=page_size,
            ):
                rd = _parse_ga_date(row.dimension_values[0].value)
                src = row.dimension_values[1].value or ""
                med = row.dimension_values[2].value or ""
                ses, au, spv = [_safe_int(mv.value) for mv in row.metric_values]
                sm_batch.append((rd, src, med, ses, au, spv))
                sm_total += 1
                if len(sm_batch) >= batch_every:
                    flush_sm(cur, sm_batch)
                    sm_batch.clear()
                    conn.commit()
            flush_sm(cur, sm_batch)
            conn.commit()

            for tup in user_rows:
                cur.execute(
                    """
                    INSERT INTO google_analytics_dm.daily_user (
                        report_date, ga_user_id, sessions,
                        screen_page_views, active_users, synced_at
                    ) VALUES (%s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (report_date, ga_user_id) DO UPDATE SET
                        sessions = EXCLUDED.sessions,
                        screen_page_views = EXCLUDED.screen_page_views,
                        active_users = EXCLUDED.active_users,
                        synced_at = NOW()
                    """,
                    tup,
                )

            page_batch: list[tuple] = []
            for row in _iter_report_rows(
                client,
                property_=prop_res,
                dimensions=[
                    {"name": "date"},
                    {"name": "pagePath"},
                    {"name": "pageTitle"},
                ],
                metrics=[
                    {"name": "screenPageViews"},
                    {"name": "sessions"},
                ],
                date_range=date_range,
                page_size=page_size,
            ):
                rd = _parse_ga_date(row.dimension_values[0].value)
                path = row.dimension_values[1].value or ""
                title = row.dimension_values[2].value or ""
                spv, ses = [_safe_int(mv.value) for mv in row.metric_values]
                page_batch.append((rd, path, title, spv, ses))
                page_total += 1
                if len(page_batch) >= batch_every:
                    flush_pages(cur, page_batch)
                    page_batch.clear()
                    conn.commit()
            flush_pages(cur, page_batch)
            conn.commit()

    print(
        "GA sync OK "
        f"range={_range_label(date_range)} | "
        f"overview={len(overview_rows)} channel={len(channel_rows)} "
        f"geo={len(geo_rows)} device={len(device_rows)} "
        f"src_med={sm_total} page_rows={page_total} user={len(user_rows)}"
    )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)

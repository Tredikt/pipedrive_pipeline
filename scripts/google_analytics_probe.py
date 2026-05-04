"""
Пробный запрос к GA4 Data API (OAuth пользователя, не service account).

  pip install -r requirements-google-analytics.txt

Подготовка:
  • GCP: включить «Google Analytics Data API» для проекта.
  • Credentials → OAuth client → тип Desktop; JSON в корень как google_oauth_client.json.
  • В .env: GA_PROPERTY — только числовой ID свойства GA4 (или properties/123…).

Запуск из корня репозитория:
  python scripts/google_analytics_probe.py

Опционально в .env: GA_REFRESH_TOKEN — без браузера (тот же OAuth-клиент).
При первом входе сохранится google_analytics_token.json в корне (не коммитить).
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

load_dotenv(_ROOT / ".env")

DATE_RANGE = {"start_date": "7daysAgo", "end_date": "today"}


def _fmt_ga_date(raw: str) -> str:
    """YYYYMMDD → YYYY-MM-DD для консоли."""
    s = (raw or "").strip()
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return s


def _fmt_duration_seconds(api_value: str) -> str:
    """GA отдаёт averageSessionDuration как число секунд (строкой)."""
    try:
        sec = float(api_value)
    except (TypeError, ValueError):
        return api_value or "—"
    if sec < 0:
        return "—"
    m, s = divmod(int(round(sec)), 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h}ч {m}м {s}с"
    if m:
        return f"{m}м {s}с"
    return f"{s}с"


def _safe_int(s: str) -> int:
    try:
        return int(float(s))
    except (TypeError, ValueError):
        return 0


def main() -> None:
    try:
        from google.analytics.data_v1beta import BetaAnalyticsDataClient
        from google.analytics.data_v1beta.types import RunReportRequest
    except ImportError as e:
        raise SystemExit(
            "Нет зависимостей GA. Установите:\n"
            "  pip install -r requirements-google-analytics.txt"
        ) from e

    raw_prop = os.environ.get("GA_PROPERTY", "").strip()
    if not raw_prop:
        raise SystemExit(
            "Задайте GA_PROPERTY в .env — числовой ID свойства GA4 "
            "(Администратор → Настройки свойства)."
        )
    prop = (
        raw_prop
        if raw_prop.startswith("properties/")
        else f"properties/{raw_prop}"
    )

    print("Property:", prop)
    print("Период: последние 7 дней (today по календарю отчёта GA4)")
    print("=" * 72)

    from src.google_analytics.auth import load_ga_oauth_credentials

    try:
        creds = load_ga_oauth_credentials()
    except FileNotFoundError as e:
        raise SystemExit(str(e)) from e
    client = BetaAnalyticsDataClient(credentials=creds)

    # --- Сводка за весь период (без разреза по дням) ---
    try:
        summary = client.run_report(
            RunReportRequest(
                property=prop,
                metrics=[
                    {"name": "activeUsers"},
                    {"name": "sessions"},
                    {"name": "screenPageViews"},
                    {"name": "newUsers"},
                    {"name": "averageSessionDuration"},
                    {"name": "eventCount"},
                ],
                date_ranges=[DATE_RANGE],
            )
        )
        if summary.rows:
            mv = summary.rows[0].metric_values
            print("\n=== Сводка за неделю (целиком, не сумма по строкам дней) ===")
            print(f"  Уникальные активные пользователи (activeUsers):  {mv[0].value}")
            print(f"  Сессии:                                          {mv[1].value}")
            print(f"  Просмотры (screenPageViews):                    {mv[2].value}")
            print(f"  Новые пользователи:                             {mv[3].value}")
            print(
                "  Средняя длительность сессии:                    "
                f"{_fmt_duration_seconds(mv[4].value)}"
            )
            print(f"  События (eventCount):                           {mv[5].value}")
            print(
                "\n  Пояснение: число activeUsers по дням в таблице ниже при суммировании\n"
                "  не даёт «уникальных за неделю» — ориентируйтесь на блок сводки выше."
            )
        else:
            print("\n(Сводка: пустой ответ)")
    except Exception as e:
        print(f"\n(Сводка недоступна: {e})")

    # --- По дням + доля просмотров от недели ---
    daily_req = RunReportRequest(
        property=prop,
        dimensions=[{"name": "date"}],
        metrics=[
            {"name": "activeUsers"},
            {"name": "sessions"},
            {"name": "screenPageViews"},
        ],
        date_ranges=[DATE_RANGE],
    )
    daily = client.run_report(daily_req)
    rows = sorted(
        daily.rows,
        key=lambda r: r.dimension_values[0].value,
    )

    print("\n=== По дням ===")
    if not rows:
        print("(нет строк)")
    else:
        total_views = sum(_safe_int(r.metric_values[2].value) for r in rows)
        hdr = (
            f"{'Дата':<12} {'Активные':>10} {'Сессии':>10} "
            f"{'Просмотры':>12} {'Доля просм.':>12}"
        )
        print(hdr)
        print("-" * len(hdr))
        for row in rows:
            raw_d = row.dimension_values[0].value
            d_iso = _fmt_ga_date(raw_d)
            u, ses, views = [mv.value for mv in row.metric_values]
            vi = _safe_int(views)
            pct = (100.0 * vi / total_views) if total_views else 0.0
            print(
                f"{d_iso:<12} {u:>10} {ses:>10} {views:>12} {pct:>11.1f}%"
            )
        print(f"\n  Всего просмотров за период (сумма по дням): {total_views}")

    # --- Каналы ---
    try:
        ch = client.run_report(
            RunReportRequest(
                property=prop,
                dimensions=[{"name": "sessionDefaultChannelGrouping"}],
                metrics=[
                    {"name": "sessions"},
                    {"name": "activeUsers"},
                    {"name": "screenPageViews"},
                ],
                date_ranges=[DATE_RANGE],
                limit=15,
            )
        )
        print("\n=== Каналы (session default channel grouping) ===")
        if not ch.rows:
            print("(пусто)")
        else:
            lst = []
            for row in ch.rows:
                name = row.dimension_values[0].value or "(не указано)"
                ses, u, v = [mv.value for mv in row.metric_values]
                lst.append((name, _safe_int(ses), _safe_int(u), _safe_int(v)))
            lst.sort(key=lambda x: x[1], reverse=True)
            print(f"{'Канал':<38} {'Сессии':>10} {'Активные':>10} {'Просмотры':>12}")
            print("-" * 72)
            for name, ses, u, v in lst[:12]:
                tn = name if len(name) <= 37 else name[:34] + "..."
                print(f"{tn:<38} {ses:>10} {u:>10} {v:>12}")
    except Exception as e:
        print(f"\n(Каналы недоступны: {e})")

    # --- Топ страниц (путь + заголовок) ---
    try:
        pages = client.run_report(
            RunReportRequest(
                property=prop,
                dimensions=[
                    {"name": "pagePath"},
                    {"name": "pageTitle"},
                ],
                metrics=[
                    {"name": "screenPageViews"},
                    {"name": "sessions"},
                ],
                date_ranges=[DATE_RANGE],
                limit=25,
            )
        )
        print("\n=== Топ страниц (по просмотрам) ===")
        if not pages.rows:
            print("(пусто)")
        else:
            ranked = []
            for row in pages.rows:
                path = row.dimension_values[0].value or ""
                title = row.dimension_values[1].value or ""
                views, ses = [mv.value for mv in row.metric_values]
                ranked.append(
                    (_safe_int(views), _safe_int(ses), path, title)
                )
            ranked.sort(key=lambda x: x[0], reverse=True)
            for i, (views, ses, path, title) in enumerate(ranked[:12], 1):
                label = (title.strip() or path or "—").replace("\n", " ")
                if len(label) > 52:
                    label = label[:49] + "..."
                path_short = path.replace("\n", " ")
                if len(path_short) > 44:
                    path_short = path_short[:41] + "..."
                print(f"  {i:2}. {views:>6} просм.  {ses:>5} сесс.  {label}")
                print(f"      {path_short}")
    except Exception as e:
        print(f"\n(Топ страниц недоступен: {e})")

    print("\n" + "=" * 72)
    print("Готово.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)

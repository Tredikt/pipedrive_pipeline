"""
Отправляет одно пробное сообщение в Slack тем же кодом, что и алерт при промахе person_identity.

Конфиг из .env: HR_MATCH_SLACK_BOT_TOKEN + HR_MATCH_SLACK_CHANNEL
или HR_MATCH_ALERT_WEBHOOK_URL (см. .env.example).

  python scripts/test_hr_match_slack_alert.py
"""
from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv

load_dotenv()

from src.identity_registry import notify_identity_match_miss


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    has_url = bool(os.environ.get("HR_MATCH_ALERT_WEBHOOK_URL", "").strip())
    has_bot = bool(os.environ.get("HR_MATCH_SLACK_BOT_TOKEN", "").strip()) and bool(
        os.environ.get("HR_MATCH_SLACK_CHANNEL", "").strip()
    )
    if not has_url and not has_bot:
        print(
            "Нет конфигурации в .env: HR_MATCH_SLACK_BOT_TOKEN + HR_MATCH_SLACK_CHANNEL "
            "или HR_MATCH_ALERT_WEBHOOK_URL",
            file=sys.stderr,
        )
        return 2

    ok = notify_identity_match_miss(
        source_system="pipedrive",
        entity_kind="manual_script_test",
        entity_id=-1,
        email="sandbox@example.com",
        detail="Пробное из scripts/test_hr_match_slack_alert.py.",
        contact_name="Иван Петров",
        crm_entity_url="https://example.com/demo-pipedrive-link",
    )
    if ok is True:
        print("Отправлено.")
        return 0
    if ok is None:
        print("Уведомление не отправлено (нет конфигурации).", file=sys.stderr)
        return 2

    print("Ошибка при отправке (см. лог выше).", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

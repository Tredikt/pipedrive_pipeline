"""
Сквозная таблица master.person_identity (sql/021 + 024).

Правило JIRA: новая строка создаётся только из PeopleForce (upsert_person_identity_from_peopleforce).
Pipedrive (и Jira webhook) только merge_person_identity_from_crm по email;
при отсутствии совпадения — master.identity_link_pending + опционально уведомление в Slack / REST
(HR_MATCH_SLACK_BOT_TOKEN+CHANNEL или запасной HR_MATCH_ALERT_WEBHOOK_URL).
"""

from __future__ import annotations

import logging
import os
from typing import Any

import psycopg
from psycopg.types.json import Json

logger = logging.getLogger(__name__)


def _clean_email(email: str | None) -> str | None:
    if email is None:
        return None
    s = str(email).strip()
    return s or None


def _clean_text(x: Any) -> str | None:
    if x is None:
        return None
    s = str(x).strip()
    return s or None


def upsert_person_identity_from_peopleforce(
    cur: psycopg.Cursor,
    *,
    email: str | None,
    full_name: str | None = None,
    first_name: str | None = None,
    last_name: str | None = None,
    peopleforce_employee_id: int | None = None,
    pipedrive_person_id: int | None = None,
    pipedrive_user_id: int | None = None,
    jira_id: str | None = None,
    google_analytics_id: str | None = None,
) -> None:
    """INSERT или UPDATE по email; из PF допускается создание строки (источник правды)."""
    em = _clean_email(email)
    if em is None:
        return
    cur.execute(
        """
        INSERT INTO master.person_identity (
            email, full_name, first_name, last_name,
            pipedrive_person_id, pipedrive_user_id,
            peopleforce_employee_id, jira_id, google_analytics_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (email_norm) DO UPDATE SET
            full_name = COALESCE(
                NULLIF(TRIM(EXCLUDED.full_name), ''),
                master.person_identity.full_name
            ),
            first_name = COALESCE(
                NULLIF(TRIM(EXCLUDED.first_name), ''),
                master.person_identity.first_name
            ),
            last_name = COALESCE(
                NULLIF(TRIM(EXCLUDED.last_name), ''),
                master.person_identity.last_name
            ),
            pipedrive_person_id = COALESCE(
                EXCLUDED.pipedrive_person_id,
                master.person_identity.pipedrive_person_id
            ),
            pipedrive_user_id = COALESCE(
                EXCLUDED.pipedrive_user_id,
                master.person_identity.pipedrive_user_id
            ),
            peopleforce_employee_id = COALESCE(
                EXCLUDED.peopleforce_employee_id,
                master.person_identity.peopleforce_employee_id
            ),
            jira_id = COALESCE(EXCLUDED.jira_id, master.person_identity.jira_id),
            google_analytics_id = COALESCE(
                EXCLUDED.google_analytics_id,
                master.person_identity.google_analytics_id
            )
        """,
        (
            em,
            _clean_text(full_name),
            _clean_text(first_name),
            _clean_text(last_name),
            pipedrive_person_id,
            pipedrive_user_id,
            peopleforce_employee_id,
            _clean_text(jira_id),
            _clean_text(google_analytics_id),
        ),
    )


def merge_person_identity_from_crm(
    cur: psycopg.Cursor,
    *,
    email: str | None,
    pipedrive_person_id: int | None = None,
    pipedrive_user_id: int | None = None,
    jira_id: str | None = None,
    full_name_hint: str | None = None,
) -> bool:
    """
    Только UPDATE существующей строки по email.
    CRM не создаёт записи. Возвращает True, если строка найдена и обновлена.
    """
    em = _clean_email(email)
    if em is None:
        return False
    jid = _clean_text(jira_id)
    cur.execute(
        """
        UPDATE master.person_identity SET
            pipedrive_person_id = COALESCE(%s, pipedrive_person_id),
            pipedrive_user_id = COALESCE(%s, pipedrive_user_id),
            jira_id = COALESCE(NULLIF(trim(%s), ''), jira_id),
            full_name = COALESCE(
                NULLIF(trim(%s), ''),
                full_name
            )
        WHERE email_norm = lower(trim(%s))
        """,
        (
            pipedrive_person_id,
            pipedrive_user_id,
            jid or "",
            _clean_text(full_name_hint) or "",
            em,
        ),
    )
    return cur.rowcount > 0


def record_identity_link_pending(
    cur: psycopg.Cursor,
    *,
    source_system: str,
    entity_kind: str,
    entity_id: int,
    email: str | None,
    detail: str | None = None,
    payload: dict[str, Any] | None = None,
) -> None:
    """Зафиксировать событие без совпадения по email (идемпотентно по системе/kind/id)."""
    em = _clean_email(email)
    if em is None:
        em = ""
    cur.execute(
        """
        INSERT INTO master.identity_link_pending (
            source_system, entity_kind, entity_id, email, detail, payload
        ) VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (source_system, entity_kind, entity_id) DO UPDATE SET
            email = EXCLUDED.email,
            detail = EXCLUDED.detail,
            payload = EXCLUDED.payload,
            created_at = NOW()
        """,
        (
            source_system.strip().lower(),
            entity_kind.strip().lower(),
            entity_id,
            em,
            detail,
            Json(payload) if payload else None,
        ),
    )


def pipedrive_ui_base_from_env() -> str | None:
    """Базовый HTTPS-URL веб‑интерфейса вида ``https://{sub}.pipedrive.com``. Без COMPANY_DOMAIN недоступен."""
    try:
        from src.config import resolve_pipedrive_api_base_url

        api = resolve_pipedrive_api_base_url().rstrip("/")
        if ".pipedrive.com/api" not in api:
            return None
        return api[: -len("/api")]
    except Exception:
        return None


def build_pipedrive_entity_url(entity_kind: str, entity_id: int) -> str | None:
    """Ссылка на карточку person/user в интерфейсе Pipedrive (если задан PIPEDRIVE_COMPANY_DOMAIN)."""
    base = pipedrive_ui_base_from_env()
    if not base:
        return None
    k = entity_kind.strip().lower()
    if k == "person":
        return f"{base}/person/{entity_id}"
    if k == "user":
        return f"{base}/settings/users/edit/{entity_id}"
    return None


def build_jira_user_profile_url(account_id: str) -> str | None:
    """Ссылка на человека в Jira Cloud: нужен ``JIRA_SITE_URL`` (напр. ``https://omnic.atlassian.net``)."""
    base = os.environ.get("JIRA_SITE_URL", "").strip().rstrip("/")
    if not base:
        return None
    aid = str(account_id).strip()
    if not aid:
        return None
    return f"{base}/jira/people/{aid}"


def _crm_display_title(source_system: str) -> str:
    s = (source_system or "").strip().lower()
    if s == "pipedrive":
        return "Pipedrive"
    if s == "jira":
        return "Jira"
    return source_system or "CRM"


def _is_slack_incoming_webhook_url(url: str) -> bool:
    return "hooks.slack.com" in url.casefold().strip()


def _post_slack_incoming(webhook_url: str, text: str) -> None:
    """Slack Incoming Webhooks принимают простой JSON `{"text": "..."}`; лишние поля могут дать ошибку."""
    import httpx

    r = httpx.post(webhook_url, json={"text": text}, timeout=15.0)
    r.raise_for_status()


def _post_slack_bot_message(token: str, channel: str, text: str) -> None:
    import httpx

    r = httpx.post(
        "https://slack.com/api/chat.postMessage",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=utf-8",
        },
        json={"channel": channel.strip(), "text": text},
        timeout=15.0,
    )
    r.raise_for_status()
    payload = r.json()
    if not payload.get("ok"):
        err = payload.get("error")
        hint = ""
        if err == "channel_not_found":
            hint = (
                " — пригласите бота в канал (/invite @ИмяПриложения) или проверьте "
                "HR_MATCH_SLACK_CHANNEL (ID C… того же workspace, без лишних пробелов)."
            )
        elif err == "not_in_channel":
            hint = " — добавьте приложение в канал (integrations или /invite @бот)."
        raise RuntimeError(
            f"Slack chat.postMessage: {err!r}{hint} (response={payload!r})"
        )


def format_identity_match_miss_alert(
    *,
    source_system: str,
    entity_kind: str,
    entity_id: int | str,
    email: str | None,
    detail: str | None = None,
    contact_name: str | None = None,
    crm_entity_url: str | None = None,
) -> tuple[str, dict[str, Any]]:
    """Текст для Slack и тело для произвольного POST-вебхука менеджера."""
    crm = _crm_display_title(source_system)
    nm = str(contact_name).strip() if contact_name else ""
    name_display = nm if nm else "имя не указано"

    email_display = (_clean_email(email) or "").strip() if email else ""
    email_display = email_display or "email не указан"

    line1 = (
        f"Внимание, в {crm} был создан контакт {name_display} "
        f"с {email_display}, однако такой email не найден. "
        f"Для корректной работы поменяйте email сотрудника на тот, что указан в PeopleForce."
    )
    parts: list[str] = [line1]
    ru: str | None = None
    if crm_entity_url:
        ru = str(crm_entity_url).strip()
        if ru:
            parts.append(f"Ссылка на страницу сотрудника — {ru}")

    text = "\n\n".join(parts)

    structured = {
        "text": text,
        "reason": "person_identity_miss",
        "source_system": source_system,
        "entity_kind": entity_kind,
        "entity_id": entity_id,
        "email": email,
        "detail": detail,
        "contact_name": contact_name,
        "crm_entity_url": ru,
        "crm_display": crm,
    }
    return text, structured


def notify_identity_match_miss(
    *,
    source_system: str,
    entity_kind: str,
    entity_id: int,
    email: str | None,
    detail: str | None = None,
    contact_name: str | None = None,
    crm_entity_url: str | None = None,
) -> bool | None:
    """
    Уведомление менеджеру при несовпадении email из CRM/Jira с базой HR.

    Порядок отправки (первое сработавшее):

    - если заданы ``HR_MATCH_SLACK_BOT_TOKEN`` и ``HR_MATCH_SLACK_CHANNEL`` — Slack ``chat.postMessage``
      (токен бота ``xoxb-…``, см. приложение Slack с scope ``chat:write``);
    - иначе, если задан ``HR_MATCH_ALERT_WEBHOOK_URL`` — Incoming Webhook (``hooks.slack.com`` только ``text``)
      или произвольный свой JSON-приёмник.

    Если ничего из этого не задано — не отправляем (воркфлоу остаётся в ``identity_link_pending``).

    Returns:
        ``None`` — не отправляли (нет конфигурации);
        ``True`` — успешно;
        ``False`` — ошибка сети/ответа (уже записано в лог).
    """
    alert_hook_url = os.environ.get("HR_MATCH_ALERT_WEBHOOK_URL", "").strip()
    slack_bot = os.environ.get("HR_MATCH_SLACK_BOT_TOKEN", "").strip()
    slack_chan = os.environ.get("HR_MATCH_SLACK_CHANNEL", "").strip()

    text, structured = format_identity_match_miss_alert(
        source_system=source_system,
        entity_kind=entity_kind,
        entity_id=entity_id,
        email=email,
        detail=detail,
        contact_name=contact_name,
        crm_entity_url=crm_entity_url,
    )

    if not alert_hook_url and not (slack_bot and slack_chan):
        return None

    try:
        import httpx

        if slack_bot and slack_chan:
            _post_slack_bot_message(slack_bot, slack_chan, text)
            logger.info(
                "HR identity alert sent (Slack chat.postMessage): source=%s channel=%s",
                source_system,
                slack_chan,
            )
            return True

        if alert_hook_url:
            if _is_slack_incoming_webhook_url(alert_hook_url):
                _post_slack_incoming(alert_hook_url, text)
                logger.info(
                    "HR identity alert sent (Slack Incoming): source=%s kind=%s",
                    source_system,
                    entity_kind,
                )
            else:
                r = httpx.post(alert_hook_url, json=structured, timeout=15.0)
                r.raise_for_status()
                logger.info(
                    "HR identity alert sent (custom webhook URL): source=%s",
                    source_system,
                )
            return True

    except Exception:
        logger.exception(
            "HR_MATCH alert notify failed (slack_pair=%s has_url=%s)",
            bool(slack_bot and slack_chan),
            bool(alert_hook_url),
        )
        return False


# Совместимость со скриптами: прежнее имя = только PF / административный upsert
def upsert_person_identity_row(
    cur: psycopg.Cursor,
    *,
    email: str | None,
    full_name: str | None = None,
    pipedrive_person_id: int | None = None,
    pipedrive_user_id: int | None = None,
    peopleforce_employee_id: int | None = None,
    jira_id: str | None = None,
    google_analytics_id: str | None = None,
    first_name: str | None = None,
    last_name: str | None = None,
) -> None:
    upsert_person_identity_from_peopleforce(
        cur,
        email=email,
        full_name=full_name,
        first_name=first_name,
        last_name=last_name,
        pipedrive_person_id=pipedrive_person_id,
        pipedrive_user_id=pipedrive_user_id,
        peopleforce_employee_id=peopleforce_employee_id,
        jira_id=jira_id,
        google_analytics_id=google_analytics_id,
    )

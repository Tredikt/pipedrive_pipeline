"""
Тестовые HTTP-запросы к вашему приёмнику PeopleForce (как в UI: POST + JSON).

Реальные вебхуки PeopleForce — всегда **POST** с телом `{"action": "...", "data": {...}}`
и опционально заголовком `x-peopleforce-signature` (HMAC-SHA256 от **сырого** тела UTF-8).

Скрипт дополнительно шлёт **PUT** и **DELETE** на тот же URL: у FastAPI по умолчанию
будет **405 Method Not Allowed** (маршрут объявлен только как @router.post).

  # поднять сервер: uvicorn src.webhook_app:app --host 127.0.0.1 --port 8000
  python scripts/test_peopleforce_webhook_http.py
  python scripts/test_peopleforce_webhook_http.py --url https://ваш-домен/peopleforce/webhook

URL по умолчанию (первый непустой):
  PEOPLEFORCE_WEBHOOK_TEST_URL,
  иначе тот же хост/схема, что у WEBHOOK_PUBLIC_URL (Pipedrive …/webhook) → …/peopleforce/webhook,
  иначе http://127.0.0.1:8000/peopleforce/webhook

Синк с PeopleForce — это API (PEOPLEFORCE_API_BASE_URL); вебхуки PeopleForce бьют в **ваш** приёмник,
как Pipedrive, обычно тот же публичный URL с другим путём.

Переменные: PEOPLEFORCE_WEBHOOK_TEST_URL, PEOPLEFORCE_WEBHOOK_SECRET (как в UI вебхука).
"""
from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv

load_dotenv()

import httpx
from urllib.parse import urlsplit, urlunsplit


def _peopleforce_url_from_pipedrive_public(pipedrive_webhook_url: str) -> str:
    """https://host/webhook -> https://host/peopleforce/webhook; …/x/webhook -> …/x/peopleforce/webhook."""
    parts = urlsplit(pipedrive_webhook_url.strip())
    path = (parts.path or "/").rstrip("/")
    if path.endswith("/webhook"):
        prefix = path[: -len("webhook")].rstrip("/")
        new_path = f"{prefix}/peopleforce/webhook" if prefix else "/peopleforce/webhook"
    else:
        new_path = f"{path}/peopleforce/webhook" if path else "/peopleforce/webhook"
    return urlunsplit((parts.scheme, parts.netloc, new_path, "", ""))


def _resolve_default_test_url() -> str:
    direct = os.environ.get("PEOPLEFORCE_WEBHOOK_TEST_URL", "").strip()
    if direct:
        return direct
    wpu = os.environ.get("WEBHOOK_PUBLIC_URL", "").strip()
    if wpu:
        return _peopleforce_url_from_pipedrive_public(wpu)
    return "http://127.0.0.1:8000/peopleforce/webhook"


def _sign_headers(secret: str, body: bytes) -> dict[str, str]:
    digest = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
    return {
        "Content-Type": "application/json",
        "x-peopleforce-signature": f"sha256={digest}",
    }


def _payload_employee(action: str, eid: int) -> dict:
    return {
        "action": action,
        "data": {
            "id": eid,
            "attributes": {
                "first_name": "Webhook",
                "last_name": "Test",
                "email": f"webhook.test.{eid}@example.invalid",
                "employee_number": f"WH{eid}",
            },
            "position": {"id": 1, "name": "Test"},
            "department": {"id": 2, "name": "QA"},
            "division": {"id": 3, "name": "HQ"},
            "location": {"id": 4, "name": "Remote"},
            "job_level": {"id": 5, "name": "L1"},
            "employment_type": {"id": 6, "name": "Full-time"},
        },
    }


def _post(
    client: httpx.Client,
    url: str,
    payload: dict,
    secret: str,
) -> httpx.Response:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    headers = _sign_headers(secret, body) if secret else {"Content-Type": "application/json"}
    return client.post(url, content=body, headers=headers)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--url",
        default=None,
        help="Полный URL; иначе см. docstring (PEOPLEFORCE_WEBHOOK_TEST_URL / WEBHOOK_PUBLIC_URL / localhost)",
    )
    p.add_argument(
        "--secret",
        default=os.environ.get("PEOPLEFORCE_WEBHOOK_SECRET", "").strip(),
        help="Секрет для подписи (или PEOPLEFORCE_WEBHOOK_SECRET); пусто — без подписи",
    )
    p.add_argument("--employee-id", type=int, default=999001, help="ID в тестовых JSON")
    p.add_argument(
        "--no-put-delete",
        action="store_true",
        help="Не слать проверочные PUT/DELETE",
    )
    args = p.parse_args()
    url = (args.url or "").strip() or _resolve_default_test_url()
    url = url.rstrip("/")
    if not url.endswith("/peopleforce/webhook") and "peopleforce" not in url:
        print("Предупреждение: ожидается путь .../peopleforce/webhook", flush=True)

    eid = args.employee_id
    sc = args.secret

    print(f"Target: {url}", flush=True)
    print(f"Signature: {'да (PEOPLEFORCE_WEBHOOK_SECRET)' if sc else 'нет (сервер примет, если secret в UI пустой)'}", flush=True)

    with httpx.Client(timeout=30.0) as client:
        for label, pl in (
            ("POST employee_create (аналог «создание»)", _payload_employee("employee_create", eid)),
            ("POST employee_update (аналог «изменение»)", _payload_employee("employee_update", eid)),
            (
                "POST employee_delete (событие удаления; не HTTP DELETE)",
                {"action": "employee_delete", "data": {"id": eid}},
            ),
        ):
            r = _post(client, url, pl, sc)
            print(f"\n--- {label} ---\n  HTTP {r.status_code}\n  {r.text[:2000]}", flush=True)

        if not args.no_put_delete:
            print("\n--- HTTP PUT (ожидается 405 — маршрут только POST) ---", flush=True)
            r = client.put(url, content=b"{}", headers={"Content-Type": "application/json"})
            print(f"  HTTP {r.status_code} {r.text[:500]}", flush=True)

            print("\n--- HTTP DELETE (ожидается 405) ---", flush=True)
            r = client.request("DELETE", url)
            print(f"  HTTP {r.status_code} {r.text[:500]}", flush=True)

    print("\nГотово.", flush=True)


if __name__ == "__main__":
    main()

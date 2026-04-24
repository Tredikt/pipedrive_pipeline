"""
Запись в **PeopleForce Public API** (app.peopleforce.io) — create / update / optional terminate.

Нужен тот же `PEOPLEFORCE_API_KEY` и `PEOPLEFORCE_API_BASE_URL`, что и для синка.
После успешного вызова PeopleForce может отправить **вебхук на ваш URL**; этот скрипт ваш приёмник не вызывает — смотрите логи/БД вебхука отдельно.

Доки: [Create](https://developer.peopleforce.io/reference/post-employees), [Update](https://developer.peopleforce.io/reference/put-employees-id), [Terminate](https://developer.peopleforce.io/reference/post-employees-id-terminate)

Примеры:

  python scripts/peopleforce_api_employee_write.py all
  python scripts/peopleforce_api_employee_write.py create
  python scripts/peopleforce_api_employee_write.py update --id 123
  # «удалить» из HR: в v3 нет DELETE /employees; только увольнение:
  python scripts/peopleforce_api_employee_write.py terminate-auto --id 704806
  # «как сможет» API: при наличии DELETE — он; иначе увольнение (terminate-auto):
  python scripts/peopleforce_api_employee_write.py remove --id 704806
  # remove: по умолчанию effective_from = вчера (UTC), чтобы увольнение уже «в прошлом»; иначе в ответе часто active=true до даты/обработки
  python scripts/peopleforce_api_employee_write.py termination-refs   # id типов/причин для ручного JSON
  python scripts/peopleforce_api_employee_write.py terminate --id 123 --json-file body.json
  # PowerShell: не подставляйте <ID1> в команду (это не плейсхолдер). Пример: --termination-type-id 7021
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv

load_dotenv()

from src.peopleforce.client import PeopleForceClient
from src.peopleforce.config import peopleforce_api_base_url, peopleforce_api_key


def _extract_employee_id(resp: Any) -> int:
    if not isinstance(resp, dict):
        raise ValueError("response is not a JSON object")
    d = resp.get("data")
    if isinstance(d, dict) and d.get("id") is not None:
        return int(d["id"])
    if resp.get("id") is not None:
        return int(resp["id"])
    raise ValueError("cannot find employee id in API response", resp)


def _client() -> PeopleForceClient:
    return PeopleForceClient(
        base_url=peopleforce_api_base_url(),
        api_key=peopleforce_api_key(),
    )


def _first_id_from_paginated(
    client: PeopleForceClient, path: str, label: str
) -> int:
    body = client.get_json(path, params={"page": 1})
    data = (body or {}).get("data") or []
    if not data or not isinstance(data[0], dict) or data[0].get("id") is None:
        raise SystemExit(
            f"Пустой или неожиданный ответ {label} ({path}); задайте terminate вручную (--json-file)."
        )
    return int(data[0]["id"])


def _resolve_termination_ids(
    client: PeopleForceClient,
    termination_type_id: int | None,
    termination_reason_id: int | None,
) -> tuple[int, int]:
    tid = termination_type_id
    rid = termination_reason_id
    if tid is None:
        tid = _first_id_from_paginated(client, "/termination_types", "termination_types")
    if rid is None:
        rid = _first_id_from_paginated(client, "/termination_reasons", "termination_reasons")
    return tid, rid


def _try_delete_employee_http(eid: int) -> bool:
    """
    Единственный вариант «настоящего» удаления в Public API, если у аккаунта/версии он появится.
    Обычно 404/405 — тогда снимаем сотрудника через /terminate.
    """
    base = peopleforce_api_base_url().rstrip("/")
    url = f"{base}/employees/{eid}"
    headers = {"X-API-KEY": peopleforce_api_key(), "Accept": "application/json"}
    with httpx.Client(timeout=60.0) as h:
        r = h.delete(url, headers=headers)
    if r.status_code in (200, 204):
        print("HTTP DELETE /employees: OK, employee removed.", flush=True)
        if r.content:
            try:
                print(json.dumps(r.json(), ensure_ascii=False, indent=2))
            except Exception:
                print(r.text[:2000], flush=True)
        return True
    snippet = (r.text or "")[:500]
    print(
        f"DELETE /employees/{eid} -> HTTP {r.status_code} ({snippet!r}...). "
        "Falling back to POST .../terminate.",
        flush=True,
    )
    return False


def _ensure_hired_on_before_terminate(
    client: PeopleForceClient,
    eid: int,
    effective_from_iso: str,
    *,
    hired_on: str | None,
    skip: bool,
) -> None:
    """
    PeopleForce отвечает 422 "Hired on can't be blank", если в карточке не было даты найма
    (часто у сотрудников, созданных только через POST /employees без hired_on).
    """
    if skip:
        return
    eff_s = (effective_from_iso or "")[:10]
    eff_d = date.fromisoformat(eff_s)
    if hired_on:
        ho = hired_on.strip()[:10]
    else:
        ho = (eff_d - timedelta(days=1)).isoformat()
    print(
        f"PUT /employees/{eid} hired_on={ho} (before terminate; PF requires hire date on profile)",
        flush=True,
    )
    try:
        client.request_json("PUT", f"/employees/{eid}", json={"hired_on": ho})
    except httpx.HTTPStatusError as e:
        print(
            f"PUT hired_on failed: HTTP {e.response.status_code} {(e.response.text or '')[:2000]}",
            flush=True,
        )
        raise SystemExit(1) from e


def _print_terminate_response_summary(r: Any) -> None:
    """PeopleForce часто возвращает полный профиль; active=true не значит «ещё в штате» (доступ к системе)."""
    if not isinstance(r, dict):
        print("Response (truncated):", str(r)[:800], flush=True)
        return
    d = r.get("data")
    if not isinstance(d, dict):
        print("Response keys:", list(r.keys()), flush=True)
        return
    parts = [
        f"id={d.get('id')}",
        f"full_name={d.get('full_name')!r}",
        f"hired_on={d.get('hired_on')}",
        f"active={d.get('active')}",
    ]
    if d.get("status") is not None:
        parts.append(f"status={d.get('status')!r}")
    for k in (
        "termination_effective_on",
        "terminated_on",
        "employment_status",
    ):
        if d.get(k) is not None:
            parts.append(f"{k}={d.get(k)!r}")
    print("POST .../terminate — HTTP 200. " + ", ".join(parts), flush=True)
    print(
        "Note: active=true часто значит доступ в PeopleForce; статус увольнения — в UI. "
        "Если effective_from = сегодня, профиль может выглядеть «активным» до конца дня/обработки.",
        flush=True,
    )


def _post_terminate_or_exit(
    client: PeopleForceClient,
    eid: int,
    body: dict[str, Any],
    *,
    verbose: bool = False,
) -> None:
    try:
        r = client.request_json("POST", f"/employees/{eid}/terminate", json=body)
    except httpx.HTTPStatusError as e:
        print(
            f"PeopleForce API: HTTP {e.response.status_code}",
            flush=True,
        )
        print((e.response.text or "")[:4000], flush=True)
        raise SystemExit(1) from e
    if r is not None:
        if verbose:
            print("Response (full JSON):\n", json.dumps(r, ensure_ascii=False, indent=2), sep="")
        else:
            _print_terminate_response_summary(r)
    else:
        print("(empty response body)", flush=True)


def _run_terminate_auto(
    client: PeopleForceClient,
    eid: int,
    *,
    effective_from: str | None,
    no_rehire: bool,
    comment: str,
    termination_type_id: int | None = None,
    termination_reason_id: int | None = None,
    hired_on: str | None = None,
    no_ensure_hired_on: bool = False,
    verbose_terminate_response: bool = False,
) -> None:
    tid, rid = _resolve_termination_ids(
        client, termination_type_id, termination_reason_id
    )
    if effective_from is not None:
        eff = effective_from.strip()
    else:
        eff = datetime.now(timezone.utc).date().isoformat()
    _ensure_hired_on_before_terminate(
        client, eid, eff, hired_on=hired_on, skip=no_ensure_hired_on
    )
    body = {
        "effective_from": eff,
        "termination_type_id": tid,
        "termination_reason_id": rid,
        "eligible_for_rehire": not no_rehire,
        "comment": comment or "",
    }
    print("Request body:\n", json.dumps(body, ensure_ascii=False, indent=2), sep="")
    _post_terminate_or_exit(
        client, eid, body, verbose=verbose_terminate_response
    )
    print(
        f"\nemployee_id={eid} — нет гарантированного hard DELETE в API; "
        f"увольнение принято при HTTP 200 (сверьте карточку в UI).",
        flush=True,
    )


def main() -> None:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    c = sub.add_parser("create", help="POST /employees")
    c.add_argument(
        "--prefix",
        default="wh-test",
        help="префикс для email / employee_number (уникальность)",
    )

    u = sub.add_parser("update", help="PUT /employees/{id}")
    u.add_argument("--id", type=int, required=True, dest="eid", help="employee id")
    u.add_argument(
        "--first-name", default="API-Updated", help="новое first_name (для заметного update)"
    )

    a = sub.add_parser("all", help="create, затем update по возвращённому id")
    a.add_argument(
        "--prefix",
        default="wh-test",
        help="префикс для create",
    )

    t = sub.add_parser(
        "terminate",
        help="POST /employees/{id}/terminate (тело — JSON, см. доку; не HTTP DELETE)",
    )
    t.add_argument("--id", type=int, required=True, dest="eid", help="employee id")
    g = t.add_mutually_exclusive_group(required=True)
    g.add_argument(
        "--json",
        dest="json_str",
        metavar="STR",
        help="JSON object как строка",
    )
    g.add_argument(
        "--json-file",
        type=Path,
        help="файл с JSON (effective_from, termination_type_id, ...)",
    )
    t.add_argument(
        "--hired-on",
        default=None,
        metavar="YYYY-MM-DD",
        help="дата найма для PUT перед terminate (иначе: день до effective_from из JSON)",
    )
    t.add_argument(
        "--no-ensure-hired-on",
        action="store_true",
        help="не делать PUT hired_on перед POST terminate",
    )
    t.add_argument(
        "--verbose",
        action="store_true",
        help="печатать полный JSON ответа POST .../terminate",
    )

    sub.add_parser(
        "termination-refs",
        help="GET /termination_types и /termination_reasons (страница 1) — для ручного terminate",
    )

    ta = sub.add_parser(
        "terminate-auto",
        help="POST /employees/{id}/terminate: первые type/reason из API + сегодняшняя дата",
    )
    ta.add_argument("--id", type=int, required=True, dest="eid", help="employee id")
    ta.add_argument(
        "--effective-from",
        default=None,
        metavar="YYYY-MM-DD",
        help="по умолчанию сегодня (UTC)",
    )
    ta.add_argument(
        "--no-rehire",
        action="store_true",
        help="eligible_for_rehire=false",
    )
    ta.add_argument(
        "--comment",
        default="terminate-auto (script cleanup)",
        help="комментарий в увольнении",
    )
    ta.add_argument(
        "--termination-type-id",
        type=int,
        default=None,
        help="иначе первый из GET /termination_types",
    )
    ta.add_argument(
        "--termination-reason-id",
        type=int,
        default=None,
        help="иначе первый из GET /termination_reasons",
    )
    ta.add_argument(
        "--hired-on",
        default=None,
        metavar="YYYY-MM-DD",
        help="PUT перед terminate: дата найма; иначе: день до --effective-from",
    )
    ta.add_argument(
        "--no-ensure-hired-on",
        action="store_true",
        help="не выставлять hired_on через PUT (если 422 — уберите флаг)",
    )
    ta.add_argument(
        "--verbose",
        action="store_true",
        help="полный JSON ответа POST terminate",
    )

    rem = sub.add_parser(
        "remove",
        help="сначала DELETE /employees/{id} (если 200/204); иначе как terminate-auto",
    )
    rem.add_argument("--id", type=int, required=True, dest="eid", help="employee id")
    rem.add_argument(
        "--effective-from",
        default=None,
        metavar="YYYY-MM-DD",
        help="дата увольнения; если не задана — вчера UTC (см. --effective-from-today)",
    )
    rem.add_argument("--no-rehire", action="store_true", help="eligible_for_rehire=false")
    rem.add_argument(
        "--comment",
        default="remove (script, best effort)",
        help="комментарий в увольнении (fallback)",
    )
    rem.add_argument(
        "--skip-delete",
        action="store_true",
        help="не вызывать HTTP DELETE, сразу terminate (как terminate-auto)",
    )
    rem.add_argument(
        "--termination-type-id",
        type=int,
        default=None,
        help="см. terminate-auto",
    )
    rem.add_argument(
        "--termination-reason-id",
        type=int,
        default=None,
        help="см. terminate-auto",
    )
    rem.add_argument(
        "--hired-on",
        default=None,
        metavar="YYYY-MM-DD",
        help="см. terminate-auto",
    )
    rem.add_argument(
        "--no-ensure-hired-on",
        action="store_true",
        help="см. terminate-auto",
    )
    rem.add_argument(
        "--verbose",
        action="store_true",
        help="см. terminate-auto",
    )
    rem.add_argument(
        "--effective-from-today",
        action="store_true",
        help="effective_from = сегодня (UTC); по умолчанию у remove — вчера",
    )

    args = p.parse_args()
    client = _client()

    if args.cmd == "create":
        tag = f"{args.prefix}-{int(time.time())}"
        ho = (datetime.now(timezone.utc).date() - timedelta(days=30)).isoformat()
        body = {
            "first_name": "API",
            "last_name": "WhTest",
            "email": f"{args.prefix}.{tag}@example.invalid",
            "employee_number": f"WT-{tag}"[:32],
            "hired_on": ho,
        }
        r = client.request_json("POST", "/employees", json=body)
        eid = _extract_employee_id(r)
        print(json.dumps(r, ensure_ascii=False, indent=2))
        print(f"\nemployee_id={eid} (смотрите вебхук employee_create / employee_update)", flush=True)
        return

    if args.cmd == "update":
        body = {"first_name": args.first_name}
        r = client.request_json("PUT", f"/employees/{args.eid}", json=body)
        if r is not None:
            print(json.dumps(r, ensure_ascii=False, indent=2))
        else:
            print("(пустой ответ 200)", flush=True)
        print(
            f"\nemployee_id={args.eid} (смотрите вебхук employee_update и т.п.)",
            flush=True,
        )
        return

    if args.cmd == "all":
        tag = f"{args.prefix}-{int(time.time())}"
        ho = (datetime.now(timezone.utc).date() - timedelta(days=30)).isoformat()
        body = {
            "first_name": "API",
            "last_name": "WhTest",
            "email": f"{args.prefix}.{tag}@example.invalid",
            "employee_number": f"WT-{tag}"[:32],
            "hired_on": ho,
        }
        cr = client.request_json("POST", "/employees", json=body)
        eid = _extract_employee_id(cr)
        print("--- create ---\n", json.dumps(cr, ensure_ascii=False, indent=2), sep="")
        body_u = {"first_name": f"API-upd-{tag}"}
        ur = client.request_json("PUT", f"/employees/{eid}", json=body_u)
        print("--- update ---\n", json.dumps(ur, ensure_ascii=False, indent=2) if ur else "(empty body)", sep="")
        print(
            f"\nemployee_id={eid} — в PeopleForce смотрите вебхуки на payload URL (create + update).",
            flush=True,
        )
        return

    if args.cmd == "terminate":
        if args.json_str is not None:
            tbody = json.loads(args.json_str)
        else:
            tbody = json.loads(args.json_file.read_text(encoding="utf-8"))
        eff = tbody.get("effective_from")
        if not isinstance(eff, str) or len(eff) < 10:
            eff = datetime.now(timezone.utc).date().isoformat()
            print(
                f"terminate: effective_from missing; using {eff} (UTC) for hired_on step",
                flush=True,
            )
        _ensure_hired_on_before_terminate(
            client,
            args.eid,
            eff,
            hired_on=args.hired_on,
            skip=args.no_ensure_hired_on,
        )
        _post_terminate_or_exit(client, args.eid, tbody, verbose=args.verbose)
        print(
            f"\nemployee_id={args.eid} — check webhooks for termination topic in UI.",
            flush=True,
        )
        return

    if args.cmd == "termination-refs":
        tt = client.get_json("/termination_types", params={"page": 1})
        tr = client.get_json("/termination_reasons", params={"page": 1})
        print("--- termination_types ---\n", json.dumps(tt, ensure_ascii=False, indent=2), sep="")
        print("--- termination_reasons ---\n", json.dumps(tr, ensure_ascii=False, indent=2), sep="")
        return

    if args.cmd == "terminate-auto":
        _run_terminate_auto(
            client,
            args.eid,
            effective_from=args.effective_from,
            no_rehire=args.no_rehire,
            comment=args.comment,
            termination_type_id=args.termination_type_id,
            termination_reason_id=args.termination_reason_id,
            hired_on=args.hired_on,
            no_ensure_hired_on=args.no_ensure_hired_on,
            verbose_terminate_response=args.verbose,
        )
        return

    if args.cmd == "remove":
        if not args.skip_delete and _try_delete_employee_http(args.eid):
            return
        eff_remove: str | None = args.effective_from
        if eff_remove is None:
            today_utc = datetime.now(timezone.utc).date()
            if args.effective_from_today:
                eff_remove = today_utc.isoformat()
                print(
                    f"remove: effective_from = today UTC ({eff_remove})",
                    flush=True,
                )
            else:
                eff_remove = (today_utc - timedelta(days=1)).isoformat()
                print(
                    f"remove: default effective_from = yesterday UTC ({eff_remove}); "
                    "termination is already in the past (clearer than 'today'). "
                    "Override: --effective-from YYYY-MM-DD or --effective-from-today",
                    flush=True,
                )
        _run_terminate_auto(
            client,
            args.eid,
            effective_from=eff_remove,
            no_rehire=args.no_rehire,
            comment=args.comment,
            termination_type_id=args.termination_type_id,
            termination_reason_id=args.termination_reason_id,
            hired_on=args.hired_on,
            no_ensure_hired_on=args.no_ensure_hired_on,
            verbose_terminate_response=args.verbose,
        )
        return


if __name__ == "__main__":
    main()

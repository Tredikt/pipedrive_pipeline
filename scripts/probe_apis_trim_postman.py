"""
GET-пробы всех list-эндпоинтов Pipedrive (ENTITY_SPECS) и PeopleForce (RAW_LIST_ENDPOINTS),
затем удаление из Postman-коллекций папок сущностей, у которых GET list вернул HTTP 404.

Требуются в .env (или окружении): PIPEDRIVE_API_TOKEN; PEOPLEFORCE_API_KEY.
При необходимости PIPEDRIVE_COMPANY_DOMAIN / PIPEDRIVE_API_BASE_URL, PEOPLEFORCE_API_BASE_URL.

Резервные копии коллекций: *.postman_collection.json.bak рядом с файлами.

  python scripts/probe_apis_trim_postman.py
  python scripts/probe_apis_trim_postman.py --dry-run
"""
from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import httpx
from dotenv import load_dotenv

load_dotenv()

from src.entities import ENTITY_SPECS
from src.config import resolve_pipedrive_api_base_url
from src.peopleforce.bulk_endpoints import RAW_LIST_ENDPOINTS, default_entity_type
from src.peopleforce.config import peopleforce_api_base_url


def _peopleforce_folder_name(path: str) -> str:
    return default_entity_type(path)


def probe_pipedrive(
    *,
    base_url: str,
    token: str,
    timeout_s: float,
) -> dict[str, int]:
    codes: dict[str, int] = {}
    with httpx.Client(timeout=timeout_s) as client:
        for spec in ENTITY_SPECS:
            limit = spec.page_size if spec.page_size is not None else 500
            limit = min(int(limit), 500)
            params = {"api_token": token, "start": 0, "limit": limit}
            p = spec.list_path if spec.list_path.startswith("/") else f"/{spec.list_path}"
            url = f"{base_url.rstrip('/')}{p}"
            r = client.get(url, params=params)
            codes[spec.name] = r.status_code
    return codes


def probe_peopleforce(
    *,
    base_url: str,
    api_key: str,
    timeout_s: float,
) -> dict[str, int]:
    codes: dict[str, int] = {}
    headers = {"X-API-KEY": api_key, "Accept": "application/json"}
    with httpx.Client(timeout=timeout_s) as client:
        for ep in RAW_LIST_ENDPOINTS:
            label = _peopleforce_folder_name(ep.path)
            path = ep.path if ep.path.startswith("/") else f"/{ep.path}"
            params: dict[str, str] = {"page": "1"}
            if ep.extra_params:
                for k, v in ep.extra_params.items():
                    params[str(k)] = str(v)
            url = f"{base_url.rstrip('/')}{path}"
            r = client.get(url, headers=headers, params=params)
            codes[label] = r.status_code
    return codes


def _backup(path: Path) -> None:
    bak = path.with_name(path.name + ".bak")
    shutil.copy2(path, bak)
    print(f"Backup: {bak}", flush=True)


def _trim_collection(
    collection_path: Path,
    remove_folder_names: set[str],
    *,
    note_prefix: str,
) -> int:
    if not remove_folder_names:
        return 0
    if not collection_path.is_file():
        print(f"Пропуск: файл коллекции не найден: {collection_path}", flush=True)
        return 0
    data = json.loads(collection_path.read_text(encoding="utf-8"))
    items = data.get("item") or []
    if not isinstance(items, list):
        return 0
    kept: list[dict] = []
    removed_count = 0
    for folder in items:
        name = folder.get("name")
        if isinstance(name, str) and name in remove_folder_names:
            removed_count += 1
            continue
        kept.append(folder)
    _backup(collection_path)
    data["item"] = kept
    desc = (data.get("info") or {}).get("description") or ""
    trim_note = (
        f"\n\n[{note_prefix}] Удалены папки (GET list → 404): {', '.join(sorted(remove_folder_names))}."
    )
    info = data.setdefault("info", {})
    info["description"] = (desc.rstrip() + trim_note).strip()
    collection_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return removed_count


def main() -> None:
    import os

    p = argparse.ArgumentParser(description="GET-пробы API и обрезка Postman по 404 на list.")
    p.add_argument("--dry-run", action="store_true", help="Только печать статусов, файлы не трогать")
    p.add_argument("--timeout", type=float, default=90.0)
    p.add_argument(
        "--pipedrive-collection",
        type=Path,
        default=_ROOT / "postman" / "Pipedrive_All_Entities.postman_collection.json",
    )
    p.add_argument(
        "--peopleforce-collection",
        type=Path,
        default=_ROOT / "postman" / "PeopleForce_All_Entities.postman_collection.json",
    )
    args = p.parse_args()

    pd_token = os.environ.get("PIPEDRIVE_API_TOKEN", "").strip()
    pf_key = os.environ.get("PEOPLEFORCE_API_KEY", "").strip()
    pd_base = resolve_pipedrive_api_base_url()
    pf_base = peopleforce_api_base_url()

    pd_404: set[str] = set()
    pf_404: set[str] = set()

    if pd_token:
        print("Pipedrive: GET list (первая страница)…", flush=True)
        pd_codes = probe_pipedrive(base_url=pd_base, token=pd_token, timeout_s=args.timeout)
        for name, code in sorted(pd_codes.items()):
            print(f"  {name}: HTTP {code}", flush=True)
            if code == 404:
                pd_404.add(name)
    else:
        print("PIPEDRIVE_API_TOKEN не задан — пропуск Pipedrive.", flush=True)

    if pf_key:
        print("PeopleForce: GET list (page=1)…", flush=True)
        pf_codes = probe_peopleforce(base_url=pf_base, api_key=pf_key, timeout_s=args.timeout)
        for name, code in sorted(pf_codes.items()):
            print(f"  {name}: HTTP {code}", flush=True)
            if code == 404:
                pf_404.add(name)
    else:
        print("PEOPLEFORCE_API_KEY не задан — пропуск PeopleForce.", flush=True)

    if args.dry_run:
        print(f"\nБудут удалены папки Postman (404): Pipedrive {sorted(pd_404)}, PeopleForce {sorted(pf_404)}", flush=True)
        return

    n_pd = _trim_collection(
        args.pipedrive_collection,
        pd_404,
        note_prefix="probe_trim",
    )
    n_pf = _trim_collection(
        args.peopleforce_collection,
        pf_404,
        note_prefix="probe_trim",
    )
    print(f"\nГотово. Удалено папок: Pipedrive {n_pd}, PeopleForce {n_pf}.", flush=True)


if __name__ == "__main__":
    main()

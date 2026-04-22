"""
Один сотрудник из PeopleForce API — сырой JSON в stdout (для сверки с DDL / маппингом).

  python scripts/dump_peopleforce_employee_sample.py
  python scripts/dump_peopleforce_employee_sample.py --id 123
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv

load_dotenv()

from src.peopleforce.client import PeopleForceClient
from src.peopleforce.config import peopleforce_api_base_url, peopleforce_api_key


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--id", type=int, default=None, help="Конкретный employee id (иначе первый со страницы)")
    args = p.parse_args()

    client = PeopleForceClient(
        base_url=peopleforce_api_base_url(),
        api_key=peopleforce_api_key(),
    )
    if args.id is not None:
        # фильтр по id (см. List employees: ids[])
        body = client.get_json(
            "/employees",
            params={"ids[]": args.id, "status": "all", "page": 1},
        )
        data = body.get("data") or []
        if not data:
            print(f"No employee with id={args.id}", file=sys.stderr)
            raise SystemExit(1)
        body = data[0]
    else:
        res = client.get_json("/employees", params={"page": 1, "status": "all"})
        rows = res.get("data") or []
        if not rows:
            print("No employees returned", file=sys.stderr)
            raise SystemExit(1)
        body = {
            "metadata": res.get("metadata"),
            "employee": rows[0],
        }

    print(json.dumps(body, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

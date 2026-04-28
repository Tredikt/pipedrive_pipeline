"""
Экспорт files/HR_Employees.csv в Excel — те же столбцы и строки, без изменений данных.

  python scripts/export_hr_employees_csv_to_xlsx.py
  python scripts/export_hr_employees_csv_to_xlsx.py --csv files/HR_Employees.csv -o files/HR_Employees.xlsx
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from openpyxl import Workbook

from src.excel_openpyxl_value import cell_value_for_openpyxl


def main() -> None:
    p = argparse.ArgumentParser(description="CSV HR_Employees → xlsx")
    p.add_argument(
        "--csv",
        type=Path,
        default=_ROOT / "files" / "HR_Employees.csv",
        help="Путь к CSV",
    )
    p.add_argument(
        "--output",
        "-o",
        type=Path,
        default=_ROOT / "files" / "HR_Employees.xlsx",
        help="Путь к .xlsx",
    )
    args = p.parse_args()

    if not args.csv.is_file():
        raise SystemExit(f"Файл не найден: {args.csv}")

    args.output.parent.mkdir(parents=True, exist_ok=True)

    wb = Workbook()
    ws = wb.active
    assert ws is not None
    ws.title = "HR_Employees"

    n = 0
    with args.csv.open(encoding="utf-8-sig", newline="") as fh:
        reader = csv.reader(fh)
        for row in reader:
            ws.append([cell_value_for_openpyxl(c) for c in row])
            n += 1

    wb.save(args.output)
    print(f"Строк (включая заголовок): {n} → {args.output}", flush=True)


if __name__ == "__main__":
    main()

"""Приведение значений из psycopg к тому, что openpyxl умеет писать в ячейку."""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any
from uuid import UUID


def cell_value_for_openpyxl(v: Any) -> Any:
    if v is None:
        return ""
    if isinstance(v, (list, tuple)):
        return json.dumps(list(v), ensure_ascii=False, default=str)
    if isinstance(v, dict):
        return json.dumps(v, ensure_ascii=False, default=str)
    if isinstance(v, (bytes, memoryview, bytearray)):
        return bytes(v).decode("utf-8", errors="replace")
    if isinstance(v, UUID):
        return str(v)
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, datetime):
        if v.tzinfo is not None:
            return v.astimezone(timezone.utc).replace(tzinfo=None)
        return v
    if isinstance(v, date):
        return v
    return v

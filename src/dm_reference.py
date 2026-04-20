"""Витрина: pipelines, stages, currencies, deal_product_line (справочники и связи deal–product)."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import psycopg

from src.db import parse_pipedrive_ts
from src.dm_crm_entities import _as_bool
from src.dm_upsert import _safe_int


def _dec(val: Any) -> Decimal | None:
    if val is None or val == "":
        return None
    try:
        return Decimal(str(val))
    except Exception:
        return None


def upsert_pipeline_dm(conn: psycopg.Connection, row: dict[str, Any]) -> None:
    pid = _safe_int(row.get("id"))
    if pid is None:
        return
    active = row.get("active")
    if active is None and "is_deleted" in row:
        active = not bool(row.get("is_deleted"))
    deal_prob = row.get("deal_probability")
    if deal_prob is None:
        deal_prob = row.get("is_deal_probability_enabled")
    selected = row.get("selected")
    if selected is None:
        selected = row.get("is_selected")
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipedrive_dm.pipeline (
                id, name, url_title, order_nr, active, deal_probability, selected,
                add_time, update_time
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                url_title = EXCLUDED.url_title,
                order_nr = EXCLUDED.order_nr,
                active = EXCLUDED.active,
                deal_probability = EXCLUDED.deal_probability,
                selected = EXCLUDED.selected,
                add_time = EXCLUDED.add_time,
                update_time = EXCLUDED.update_time,
                synced_at = NOW();
            """,
            (
                pid,
                row.get("name"),
                row.get("url_title"),
                _safe_int(row.get("order_nr")),
                _as_bool(active),
                _as_bool(deal_prob),
                _as_bool(selected),
                parse_pipedrive_ts(row.get("add_time")),
                parse_pipedrive_ts(row.get("update_time")),
            ),
        )


def upsert_stage_dm(conn: psycopg.Connection, row: dict[str, Any]) -> None:
    sid = _safe_int(row.get("id"))
    if sid is None:
        return
    pipeline_id = _safe_int(row.get("pipeline_id"))
    active = row.get("active_flag")
    if active is None:
        active = row.get("active")
    if active is None and "is_deleted" in row:
        active = not bool(row.get("is_deleted"))
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipedrive_dm.stage (
                id, pipeline_id, name, order_nr, deal_probability, rotten_days,
                rotten_flag, active_flag, add_time, update_time
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                pipeline_id = EXCLUDED.pipeline_id,
                name = EXCLUDED.name,
                order_nr = EXCLUDED.order_nr,
                deal_probability = EXCLUDED.deal_probability,
                rotten_days = EXCLUDED.rotten_days,
                rotten_flag = EXCLUDED.rotten_flag,
                active_flag = EXCLUDED.active_flag,
                add_time = EXCLUDED.add_time,
                update_time = EXCLUDED.update_time,
                synced_at = NOW();
            """,
            (
                sid,
                pipeline_id,
                row.get("name"),
                _safe_int(row.get("order_nr")),
                _dec(row.get("deal_probability")),
                _safe_int(row.get("rotten_days")),
                _as_bool(row.get("rotten_flag")),
                _as_bool(active),
                parse_pipedrive_ts(row.get("add_time")),
                parse_pipedrive_ts(row.get("update_time")),
            ),
        )


def upsert_currency_dm(conn: psycopg.Connection, row: dict[str, Any]) -> None:
    cid = _safe_int(row.get("id"))
    if cid is None:
        return
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipedrive_dm.currency (
                id, code, name, decimal_points, symbol, active_flag,
                is_default, is_custom, symbol_before_amount
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                code = EXCLUDED.code,
                name = EXCLUDED.name,
                decimal_points = EXCLUDED.decimal_points,
                symbol = EXCLUDED.symbol,
                active_flag = EXCLUDED.active_flag,
                is_default = EXCLUDED.is_default,
                is_custom = EXCLUDED.is_custom,
                symbol_before_amount = EXCLUDED.symbol_before_amount,
                synced_at = NOW();
            """,
            (
                cid,
                row.get("code"),
                row.get("name"),
                _safe_int(row.get("decimal_points")),
                row.get("symbol"),
                _as_bool(row.get("active_flag")),
                _as_bool(row.get("is_default")),
                _as_bool(row.get("is_custom")),
                _as_bool(row.get("symbol_before_amount")),
            ),
        )


def upsert_deal_product_dm(conn: psycopg.Connection, row: dict[str, Any]) -> None:
    lid = _safe_int(row.get("id"))
    if lid is None:
        return
    deal_id = _safe_int(row.get("deal_id"))
    if deal_id is None:
        return
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipedrive_dm.deal_product_line (
                id, deal_id, product_id, name, quantity, item_price, sum, currency,
                product_variation_id, discount_percentage, duration, duration_unit,
                tax, tax_method, enabled_flag, add_time, update_time
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (id) DO UPDATE SET
                deal_id = EXCLUDED.deal_id,
                product_id = EXCLUDED.product_id,
                name = EXCLUDED.name,
                quantity = EXCLUDED.quantity,
                item_price = EXCLUDED.item_price,
                sum = EXCLUDED.sum,
                currency = EXCLUDED.currency,
                product_variation_id = EXCLUDED.product_variation_id,
                discount_percentage = EXCLUDED.discount_percentage,
                duration = EXCLUDED.duration,
                duration_unit = EXCLUDED.duration_unit,
                tax = EXCLUDED.tax,
                tax_method = EXCLUDED.tax_method,
                enabled_flag = EXCLUDED.enabled_flag,
                add_time = EXCLUDED.add_time,
                update_time = EXCLUDED.update_time,
                synced_at = NOW();
            """,
            (
                lid,
                deal_id,
                _safe_int(row.get("product_id")),
                row.get("name"),
                _dec(row.get("quantity")),
                _dec(row.get("item_price")),
                _dec(row.get("sum")),
                row.get("currency"),
                _safe_int(row.get("product_variation_id")),
                _dec(row.get("discount_percentage")),
                _safe_int(row.get("duration")),
                row.get("duration_unit") if isinstance(row.get("duration_unit"), str) else None,
                _dec(row.get("tax")),
                row.get("tax_method") if isinstance(row.get("tax_method"), str) else None,
                _as_bool(row.get("enabled_flag")),
                parse_pipedrive_ts(row.get("add_time")),
                parse_pipedrive_ts(row.get("update_time")),
            ),
        )

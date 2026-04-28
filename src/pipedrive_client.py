from __future__ import annotations

import json
import logging
from typing import Any, Iterator

import httpx

logger = logging.getLogger(__name__)


class PipedriveEndpointUnreadableError(RuntimeError):
    """HTTP 200, но тело не JSON (прокси/HTML/обрыв). Синк может пропустить эндпоинт."""


def pipedrive_list_next_start(
    body: dict[str, Any],
    *,
    start: int,
    page_size: int,
    row_count: int,
) -> int | None:
    """
    Следующий start для start/limit или None, если страниц больше нет.

    Если more_items_in_collection не пришёл, не обрываем выгрузку на первой странице,
    а продолжаем, пока пришла «полная» порция (как next_start/offset).
    """
    pag = (body.get("additional_data") or {}).get("pagination") or {}
    more = pag.get("more_items_in_collection")
    if more is True:
        ns = pag.get("next_start")
        return int(ns) if ns is not None else start + row_count
    if more is False:
        return None
    # more is None: нестандартный/старый ответ
    if row_count == 0:
        return None
    if row_count < page_size:
        return None
    ns = pag.get("next_start")
    if ns is not None:
        return int(ns)
    logger.debug(
        "Pipedrive pagination: more_items_in_collection missing, inferring next from row_count path=%s",
        body.get("additional_data"),
    )
    return start + row_count


class PipedriveClient:
    def __init__(self, *, base_url: str, api_token: str, timeout_s: float = 60.0) -> None:
        self._base = base_url.rstrip("/")
        self._token = api_token
        self._timeout = timeout_s

    def get_json(self, path: str, *, params: dict[str, Any] | None = None) -> dict[str, Any]:
        # api_token только в params: иначе httpx при merge с params затирает query-string URL.
        p = path if path.startswith("/") else f"/{path}"
        url = f"{self._base}{p}"
        merged: dict[str, Any] = {"api_token": self._token}
        if params:
            merged.update(params)
        with httpx.Client(timeout=self._timeout) as client:
            r = client.get(url, params=merged)
            r.raise_for_status()
            raw = r.content or b""
            if not raw.strip():
                logger.warning(
                    "Pipedrive GET %s: пустое тело (HTTP %s)",
                    p,
                    r.status_code,
                )
                return {"success": True, "data": None}
            try:
                return json.loads(raw.decode("utf-8", errors="replace"))
            except json.JSONDecodeError as e:
                preview = raw[:400].decode("utf-8", errors="replace")
                logger.warning(
                    "Pipedrive GET %s: не JSON (HTTP %s): %s; начало ответа: %r",
                    p,
                    r.status_code,
                    e,
                    preview,
                )
                raise PipedriveEndpointUnreadableError(
                    f"{p}: ответ не JSON (HTTP {r.status_code})"
                ) from e

    def post_json(self, path: str, *, json_body: dict[str, Any]) -> dict[str, Any]:
        """POST JSON (например POST /v1/deals). api_token в query."""
        p = path if path.startswith("/") else f"/{path}"
        url = f"{self._base}{p}"
        params = {"api_token": self._token}
        with httpx.Client(timeout=self._timeout) as client:
            r = client.post(url, params=params, json=json_body)
            r.raise_for_status()
            return r.json()

    def put_json(self, path: str, *, json_body: dict[str, Any]) -> dict[str, Any]:
        """PUT JSON (например PUT /v1/deals/123). api_token в query."""
        p = path if path.startswith("/") else f"/{path}"
        url = f"{self._base}{p}"
        params = {"api_token": self._token}
        with httpx.Client(timeout=self._timeout) as client:
            r = client.put(url, params=params, json=json_body)
            r.raise_for_status()
            return r.json()

    def delete_item(self, path: str) -> dict[str, Any]:
        """DELETE (например DELETE /v1/deals/123). api_token в query."""
        p = path if path.startswith("/") else f"/{path}"
        url = f"{self._base}{p}"
        params = {"api_token": self._token}
        with httpx.Client(timeout=self._timeout) as client:
            r = client.delete(url, params=params)
            r.raise_for_status()
            if r.content:
                return r.json()
            return {"success": True}

    def iter_collection(
        self,
        path: str,
        *,
        page_size: int = 500,
        extra_params: dict[str, Any] | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Пагинация start/limit (offset), см. https://pipedrive.readme.io/docs/core-api-concepts-pagination — limit до 500."""
        start = 0
        while True:
            params: dict[str, Any] = {"start": start, "limit": page_size}
            if extra_params:
                params.update(extra_params)
            body = self.get_json(path, params=params)
            if not body.get("success", True):
                raise RuntimeError(f"Pipedrive error for {path}: {body}")
            data = body.get("data")
            rows = _normalize_data(data)
            yield from rows
            nxt = pipedrive_list_next_start(
                body, start=start, page_size=page_size, row_count=len(rows)
            )
            if nxt is None:
                break
            start = nxt

    def get_item(self, path: str, item_id: str | int) -> dict[str, Any] | None:
        """GET одной сущности: /v1/deals/123 → data."""
        p = path if path.startswith("/") else f"/{path}"
        url_path = f"{p.rstrip('/')}/{item_id}"
        try:
            body = self.get_json(url_path)
        except httpx.HTTPStatusError as e:
            code = e.response.status_code
            if code == 404:
                return None
            # Нет доступа к объекту или неверный id — для webhooks не даём 500, только «нет строки».
            if code in (400, 401, 403):
                logger.warning(
                    "get_item HTTP %s path=%s id=%s",
                    code,
                    url_path,
                    item_id,
                )
                return None
            raise
        if not body.get("success", True):
            return None
        data = body.get("data")
        if isinstance(data, dict):
            return data
        return None


def _normalize_data(data: Any) -> list[dict[str, Any]]:
    if data is None:
        return []
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        # Иногда API отдаёт объект с числовыми ключами
        out: list[dict[str, Any]] = []
        for v in data.values():
            if isinstance(v, dict):
                out.append(v)
        return out
    return []

from __future__ import annotations

from typing import Any, Iterator

import httpx


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
            return r.json()

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
            pag = (body.get("additional_data") or {}).get("pagination") or {}
            if not pag.get("more_items_in_collection"):
                break
            start = int(pag.get("next_start", start + page_size))

    def get_item(self, path: str, item_id: str | int) -> dict[str, Any] | None:
        """GET одной сущности: /v1/deals/123 → data."""
        p = path if path.startswith("/") else f"/{path}"
        url_path = f"{p.rstrip('/')}/{item_id}"
        try:
            body = self.get_json(url_path)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
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

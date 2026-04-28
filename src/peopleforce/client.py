from __future__ import annotations

from typing import Any, Iterator

import httpx


def _peopleforce_list_total_pages(body: dict[str, Any]) -> int:
    """
    В ответе list API v3 total pages лежит в metadata.pagination.pages
    (а не в metadata.pages). Без вложенного ключа пагинация «ломается» в 1 страницу.
    """
    meta = body.get("metadata")
    if not isinstance(meta, dict):
        return 1
    pag = meta.get("pagination")
    if isinstance(pag, dict):
        v = pag.get("pages")
        if v is not None:
            try:
                n = int(v)
            except (TypeError, ValueError):
                n = 1
            return max(0, n)
    v = meta.get("pages")
    if v is not None:
        try:
            return max(0, int(v))
        except (TypeError, ValueError):
            pass
    return 1


class PeopleForceClient:
    """Клиент PeopleForce public API v3: заголовок X-API-KEY."""

    def __init__(
        self,
        *,
        base_url: str,
        api_key: str,
        timeout_s: float = 60.0,
    ) -> None:
        self._base = base_url.rstrip("/")
        self._key = api_key
        self._timeout = timeout_s

    def request_json(
        self,
        method: str,
        path: str,
        *,
        json: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
    ) -> Any:
        """POST/PUT/PATCH/DELETE в Public API. Заголовок X-API-KEY."""
        p = path if path.startswith("/") else f"/{path}"
        url = f"{self._base}{p}"
        headers: dict[str, str] = {
            "X-API-KEY": self._key,
            "Accept": "application/json",
        }
        if json is not None:
            headers["Content-Type"] = "application/json"
        m = method.strip().upper()
        with httpx.Client(timeout=self._timeout) as client:
            r = client.request(
                m,
                url,
                headers=headers,
                params=params,
                json=json,
            )
            r.raise_for_status()
        if not (r.content and r.content.strip()):
            return None
        return r.json()

    def get_json(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        raise_on_error: bool = True,
    ) -> dict[str, Any] | None:
        p = path if path.startswith("/") else f"/{path}"
        url = f"{self._base}{p}"
        headers = {"X-API-KEY": self._key, "Accept": "application/json"}
        with httpx.Client(timeout=self._timeout) as client:
            r = client.get(url, headers=headers, params=params or {})
            if not r.is_success:
                if (
                    not raise_on_error
                    and r.status_code in (401, 403, 404)
                ):
                    return None
                r.raise_for_status()
            return r.json()

    def iter_paginated(
        self,
        path: str,
        *,
        extra_params: dict[str, Any] | None = None,
        skip_on_client_error: bool = False,
    ) -> Iterator[dict[str, Any]]:
        """
        Список: data[] + metadata.pagination (page, pages, count, items).
        Цикл page=1..metadata.pagination.pages. См. PeopleForce docs/pagination.
        """
        page = 1
        while True:
            params: dict[str, Any] = {"page": page}
            if extra_params:
                params.update(extra_params)
            body = self.get_json(
                path, params=params, raise_on_error=not skip_on_client_error
            )
            if body is None:
                return
            data = body.get("data")
            rows: list[dict[str, Any]] = data if isinstance(data, list) else []
            yield from rows
            total_pages = _peopleforce_list_total_pages(body)
            if total_pages <= 0:
                total_pages = 1
            if page >= total_pages:
                break
            page += 1

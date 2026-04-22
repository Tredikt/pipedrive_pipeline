from __future__ import annotations

from typing import Any, Iterator

import httpx


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
        Список с data[] + metadata.pages (PeopleForce pagination page=1..n).
        См. https://developer.peopleforce.io/docs/pagination
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
            meta = body.get("metadata") or {}
            pages = int(meta.get("pages") or 1)
            if page >= pages or not rows:
                break
            page += 1

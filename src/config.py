from __future__ import annotations

import os
from dataclasses import dataclass
from urllib.parse import urlparse

from dotenv import load_dotenv

load_dotenv()


def _normalize_company_domain(raw: str) -> str:
    """Поддомен компании: omnic-6566d9 или полный хост."""
    s = raw.strip().rstrip("/")
    if "://" in s or ".pipedrive.com" in s and "://" not in s:
        if "://" not in s:
            s = f"https://{s}"
        host = (urlparse(s).hostname or "").lower()
        if host.endswith(".pipedrive.com"):
            return host[: -len(".pipedrive.com")].strip(".")
        return host.split(".")[0] if host else raw.strip()
    return s.strip(".")


def resolve_pipedrive_api_base_url() -> str:
    """
    Домен компании: https://{subdomain}.pipedrive.com/api/v1/...
    Центральный хост: https://api.pipedrive.com/v1/...
    См. https://developers.pipedrive.com/docs/api/v1
    """
    domain = os.environ.get("PIPEDRIVE_COMPANY_DOMAIN", "").strip()
    if domain:
        sub = _normalize_company_domain(domain)
        if not sub:
            raise RuntimeError("PIPEDRIVE_COMPANY_DOMAIN is empty after normalization")
        return f"https://{sub}.pipedrive.com/api"
    return os.environ.get("PIPEDRIVE_API_BASE_URL", "https://api.pipedrive.com").rstrip("/")


def get_database_url() -> str:
    """Только PostgreSQL — для --init-db / --schema-only без токена Pipedrive."""
    db_url = os.environ.get("DATABASE_URL", "").strip()
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set")
    return db_url


@dataclass(frozen=True)
class Settings:
    pipedrive_api_token: str
    pipedrive_api_base_url: str
    database_url: str


def get_settings() -> Settings:
    token = os.environ.get("PIPEDRIVE_API_TOKEN", "").strip()
    if not token:
        raise RuntimeError("PIPEDRIVE_API_TOKEN is not set")
    base = resolve_pipedrive_api_base_url()
    return Settings(
        pipedrive_api_token=token,
        pipedrive_api_base_url=base,
        database_url=get_database_url(),
    )

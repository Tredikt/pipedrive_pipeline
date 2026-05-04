"""OAuth 2.0 (Desktop) для GA Data API — общий код для probe и sync."""

from __future__ import annotations

import json
import os
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_OAUTH_CLIENT = _REPO_ROOT / "google_oauth_client.json"
_TOKEN_FILE = _REPO_ROOT / "google_analytics_token.json"

SCOPES = ("https://www.googleapis.com/auth/analytics.readonly",)


def oauth_client_path() -> Path:
    return _OAUTH_CLIENT


def token_path() -> Path:
    return _TOKEN_FILE


def _env_refresh_token() -> str | None:
    v = os.environ.get("GA_REFRESH_TOKEN", "").strip()
    return v or None


def _credentials_from_refresh(refresh_token: str):
    from google.oauth2.credentials import Credentials

    raw = json.loads(_OAUTH_CLIENT.read_text(encoding="utf-8"))
    inst = raw.get("installed") or raw.get("web")
    if not inst:
        raise RuntimeError(
            "В google_oauth_client.json нужен объект «installed» (Desktop) или «web»."
        )
    token_uri = inst.get("token_uri") or "https://oauth2.googleapis.com/token"
    return Credentials(
        token=None,
        refresh_token=refresh_token.strip(),
        token_uri=token_uri,
        client_id=inst["client_id"],
        client_secret=inst["client_secret"],
        scopes=list(SCOPES),
    )


def load_ga_oauth_credentials():
    """Credentials для BetaAnalyticsDataClient (браузер / token.json / GA_REFRESH_TOKEN)."""
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow

    if not _OAUTH_CLIENT.is_file():
        raise FileNotFoundError(
            f"Нет файла {_OAUTH_CLIENT} — OAuth Desktop JSON из Google Cloud Console."
        )

    creds: Credentials | None = None
    if _TOKEN_FILE.is_file():
        creds = Credentials.from_authorized_user_file(str(_TOKEN_FILE), list(SCOPES))

    if creds and creds.valid:
        return creds

    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
        _TOKEN_FILE.write_text(creds.to_json(), encoding="utf-8")
        return creds

    rt_only = _env_refresh_token()
    if rt_only:
        creds = _credentials_from_refresh(rt_only)
        creds.refresh(Request())
        _TOKEN_FILE.write_text(creds.to_json(), encoding="utf-8")
        return creds

    flow = InstalledAppFlow.from_client_secrets_file(
        str(_OAUTH_CLIENT),
        list(SCOPES),
    )
    creds = flow.run_local_server(port=0, open_browser=True)
    _TOKEN_FILE.write_text(creds.to_json(), encoding="utf-8")
    return creds

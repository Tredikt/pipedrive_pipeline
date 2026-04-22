from __future__ import annotations

import os

from dotenv import load_dotenv

load_dotenv()

DEFAULT_BASE = "https://app.peopleforce.io/api/public/v3"


def peopleforce_api_base_url() -> str:
    return os.environ.get("PEOPLEFORCE_API_BASE_URL", DEFAULT_BASE).rstrip("/")


def peopleforce_api_key() -> str:
    k = os.environ.get("PEOPLEFORCE_API_KEY", "").strip()
    if not k:
        raise RuntimeError("PEOPLEFORCE_API_KEY is not set")
    return k


def peopleforce_webhook_secret() -> str:
    return os.environ.get("PEOPLEFORCE_WEBHOOK_SECRET", "").strip()

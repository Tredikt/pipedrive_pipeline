#!/bin/sh
set -eu

# Собираем один файл для cron из всех docker/cron.d/*.cron (расширяйте новыми фрагментами).
#
# Важно: демон cron не пробрасывает в дочерние задания весь ENV контейнера (docker compose env_file),
# из‑за этого в Python не попадёт DATABASE_URL. Перед cron -f сохраняем allowlist в /run/docker-cron-env.sh
# и подключаем его в docker/cron/*.sh.

CRON_OUT=/etc/cron.d/app-crontabs

python3 <<'PY'
"""Экспортируемые строки для `set -a; . файл` / обычного `. файл` в sh."""
import os
import shlex

_KEYS = (
    "DATABASE_URL",
    "GA_PROPERTY",
    "GA_REFRESH_TOKEN",
    "GA_SYNC_DAYS",
    "GA_SYNC_EXTRA_ARGS",
    "GA_SYNC_FULL_HISTORY",
    "GA_SYNC_START_DATE",
    "GA_SYNC_SKIP_CHANNEL",
    "GA_SYNC_USER_DIMENSION",
    "GA_SYNC_USER_CUSTOM_DIMENSION",
    "GA_SYNC_PAGE_LIMIT",
    "PIPEDRIVE_API_TOKEN",
    "PIPEDRIVE_COMPANY_DOMAIN",
    "PIPEDRIVE_API_BASE_URL",
)

lines = ["# Автосгенерировано entrypoint-cron.sh; только для задач cron в этом контейнере."]
for k in _KEYS:
    v = os.environ.get(k)
    if v is None or str(v).strip() == "":
        continue
    lines.append(f"export {k}={shlex.quote(str(v))}")

path = "/run/docker-cron-env.sh"
umask = os.umask(0o077)
try:
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
finally:
    os.umask(umask)
PY

{
  printf '%s\n' \
    '# app-crontabs — см. репозиторий docker/cron.d/*.cron' \
    'SHELL=/bin/sh' \
    'PATH=/usr/local/bin:/usr/bin:/bin' \
    ''

  if [ -d /app/docker/cron.d ]; then
    find /app/docker/cron.d -maxdepth 1 -type f -name '*.cron' \
      | sort \
      | while IFS= read -r f; do
          cat "$f"
          echo ""
        done
  fi
} > "$CRON_OUT"

chmod 0644 "$CRON_OUT"

exec cron -f

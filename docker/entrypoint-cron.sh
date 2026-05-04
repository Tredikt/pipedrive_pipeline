#!/bin/sh
set -eu

# Собираем один файл для cron из всех docker/cron.d/*.cron (расширяйте новыми фрагментами).

CRON_OUT=/etc/cron.d/app-crontabs

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

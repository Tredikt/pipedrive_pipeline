#!/bin/sh
set -eu
cd /app
if [ -f /run/docker-cron-env.sh ]; then
  # shellcheck source=/run/docker-cron-env.sh
  . /run/docker-cron-env.sh
fi
exec python -m src.google_analytics.sync --days "${GA_SYNC_DAYS:-2}" ${GA_SYNC_EXTRA_ARGS:-}

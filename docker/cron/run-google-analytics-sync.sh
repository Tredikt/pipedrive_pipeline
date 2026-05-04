#!/bin/sh
set -eu
cd /app
exec python -m src.google_analytics.sync --days "${GA_SYNC_DAYS:-2}" ${GA_SYNC_EXTRA_ARGS:-}

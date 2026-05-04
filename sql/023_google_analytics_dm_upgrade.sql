-- Обновление со старой схемы 022 (колонка ga_property_id): идемпотентно.

CREATE SCHEMA IF NOT EXISTS google_analytics_dm;

DROP VIEW IF EXISTS google_analytics_dm.v_daily_user_person CASCADE;

-- Удалить PK по имени динамически
DO $$
DECLARE
    con TEXT;
BEGIN
  IF EXISTS (
    SELECT 1
    FROM pg_attribute a
    JOIN pg_class c ON c.oid = a.attrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'google_analytics_dm'
      AND c.relname = 'daily_overview'
      AND c.relkind IN ('r', 'p')
      AND a.attnum > 0
      AND NOT a.attisdropped
      AND a.attname = 'ga_property_id'
  ) THEN

    SELECT c.conname INTO con
    FROM pg_constraint c
    JOIN pg_class t ON c.conrelid = t.oid
    JOIN pg_namespace n ON t.relnamespace = n.oid
    WHERE n.nspname = 'google_analytics_dm'
      AND t.relname = 'daily_overview'
      AND c.contype = 'p';
    IF con IS NOT NULL THEN
      EXECUTE format(
        'ALTER TABLE google_analytics_dm.daily_overview DROP CONSTRAINT %I',
        con
      );
    END IF;
    ALTER TABLE google_analytics_dm.daily_overview DROP COLUMN ga_property_id;
    ALTER TABLE google_analytics_dm.daily_overview
      ADD PRIMARY KEY (report_date);

    SELECT c.conname INTO con
    FROM pg_constraint c
    JOIN pg_class t ON c.conrelid = t.oid
    JOIN pg_namespace n ON t.relnamespace = n.oid
    WHERE n.nspname = 'google_analytics_dm'
      AND t.relname = 'daily_channel'
      AND c.contype = 'p';
    IF con IS NOT NULL THEN
      EXECUTE format(
        'ALTER TABLE google_analytics_dm.daily_channel DROP CONSTRAINT %I',
        con
      );
    END IF;
    ALTER TABLE google_analytics_dm.daily_channel DROP COLUMN ga_property_id;
    ALTER TABLE google_analytics_dm.daily_channel
      ADD PRIMARY KEY (report_date, channel);

    SELECT c.conname INTO con
    FROM pg_constraint c
    JOIN pg_class t ON c.conrelid = t.oid
    JOIN pg_namespace n ON t.relnamespace = n.oid
    WHERE n.nspname = 'google_analytics_dm'
      AND t.relname = 'daily_page'
      AND c.contype = 'p';
    IF con IS NOT NULL THEN
      EXECUTE format(
        'ALTER TABLE google_analytics_dm.daily_page DROP CONSTRAINT %I',
        con
      );
    END IF;
    ALTER TABLE google_analytics_dm.daily_page DROP COLUMN ga_property_id;
    ALTER TABLE google_analytics_dm.daily_page
      ADD PRIMARY KEY (report_date, page_path);

    SELECT c.conname INTO con
    FROM pg_constraint c
    JOIN pg_class t ON c.conrelid = t.oid
    JOIN pg_namespace n ON t.relnamespace = n.oid
    WHERE n.nspname = 'google_analytics_dm'
      AND t.relname = 'daily_user'
      AND c.contype = 'p';
    IF con IS NOT NULL THEN
      EXECUTE format(
        'ALTER TABLE google_analytics_dm.daily_user DROP CONSTRAINT %I',
        con
      );
    END IF;
    ALTER TABLE google_analytics_dm.daily_user DROP COLUMN ga_property_id;
    ALTER TABLE google_analytics_dm.daily_user
      ADD PRIMARY KEY (report_date, ga_user_id);
  END IF;
END $$;

CREATE TABLE IF NOT EXISTS google_analytics_dm.daily_geo (
    report_date        DATE NOT NULL,
    country            TEXT NOT NULL,
    sessions           BIGINT,
    active_users       BIGINT,
    screen_page_views  BIGINT,
    synced_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (report_date, country)
);

CREATE TABLE IF NOT EXISTS google_analytics_dm.daily_device (
    report_date        DATE NOT NULL,
    device_category    TEXT NOT NULL,
    sessions           BIGINT,
    active_users       BIGINT,
    screen_page_views  BIGINT,
    synced_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (report_date, device_category)
);

CREATE TABLE IF NOT EXISTS google_analytics_dm.daily_source_medium (
    report_date        DATE NOT NULL,
    session_source     TEXT NOT NULL DEFAULT '',
    session_medium     TEXT NOT NULL DEFAULT '',
    sessions           BIGINT,
    active_users       BIGINT,
    screen_page_views  BIGINT,
    synced_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (report_date, session_source, session_medium)
);

DROP VIEW IF EXISTS google_analytics_dm.v_daily_user_person CASCADE;

CREATE OR REPLACE VIEW google_analytics_dm.v_daily_user_person AS
SELECT
    u.report_date,
    u.ga_user_id,
    u.sessions,
    u.screen_page_views,
    u.active_users,
    u.synced_at,
    p.id AS person_identity_id,
    p.email,
    p.full_name
FROM google_analytics_dm.daily_user u
LEFT JOIN master.person_identity p
    ON p.google_analytics_id IS NOT NULL
   AND trim(p.google_analytics_id) <> ''
   AND p.google_analytics_id = u.ga_user_id;

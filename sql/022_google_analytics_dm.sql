-- Витрина GA4 Data API (агрегаты по календарному дню; один объект недвижимости на БД).
-- Полный исторический диапазон задаётся при синке (GA_SYNC_START_DATE … today).
-- Связка пользователей: daily_user.ga_user_id = master.person_identity.google_analytics_id

-- Требуется master.person_identity для представления v_daily_user_person (sql/021).

CREATE SCHEMA IF NOT EXISTS google_analytics_dm;

CREATE TABLE IF NOT EXISTS google_analytics_dm.daily_overview (
    report_date        DATE NOT NULL,
    active_users       BIGINT,
    sessions           BIGINT,
    screen_page_views  BIGINT,
    new_users          BIGINT,
    synced_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (report_date)
);

CREATE INDEX IF NOT EXISTS ix_ga_daily_overview_date
    ON google_analytics_dm.daily_overview (report_date DESC);

CREATE TABLE IF NOT EXISTS google_analytics_dm.daily_channel (
    report_date        DATE NOT NULL,
    channel            TEXT NOT NULL,
    sessions           BIGINT,
    active_users       BIGINT,
    screen_page_views  BIGINT,
    synced_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (report_date, channel)
);

CREATE INDEX IF NOT EXISTS ix_ga_daily_channel_date
    ON google_analytics_dm.daily_channel (report_date DESC);

CREATE TABLE IF NOT EXISTS google_analytics_dm.daily_page (
    report_date        DATE NOT NULL,
    page_path          TEXT NOT NULL,
    page_title         TEXT NOT NULL DEFAULT '',
    screen_page_views  BIGINT,
    sessions           BIGINT,
    synced_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (report_date, page_path)
);

CREATE INDEX IF NOT EXISTS ix_ga_daily_page_date
    ON google_analytics_dm.daily_page (report_date DESC);

CREATE TABLE IF NOT EXISTS google_analytics_dm.daily_user (
    report_date        DATE NOT NULL,
    ga_user_id         TEXT NOT NULL,
    sessions           BIGINT,
    screen_page_views  BIGINT,
    active_users       BIGINT,
    synced_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (report_date, ga_user_id)
);

CREATE INDEX IF NOT EXISTS ix_ga_daily_user_date
    ON google_analytics_dm.daily_user (report_date DESC);

-- Дополнительные разрезы («вложенность» по измерениям GA)
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

COMMENT ON SCHEMA google_analytics_dm IS 'Отчёты GA4 Data API; обновление по расписанию';

-- REPLACE VIEW нельзя использовать для удаления колонок из существующего представления.
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

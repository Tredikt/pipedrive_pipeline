-- Разворачивание полей кандидата (API recruitment/candidates) из payload в отдельные колонки.
-- Безопасно для БД, созданных по старому 010: ADD COLUMN IF NOT EXISTS.

ALTER TABLE peopleforce_dm.recruitment_candidate
    ADD COLUMN IF NOT EXISTS full_name     TEXT,
    ADD COLUMN IF NOT EXISTS tags         TEXT[],
    ADD COLUMN IF NOT EXISTS urls          TEXT[],
    ADD COLUMN IF NOT EXISTS level         TEXT,
    ADD COLUMN IF NOT EXISTS gender        TEXT,
    ADD COLUMN IF NOT EXISTS gender_id     BIGINT,
    ADD COLUMN IF NOT EXISTS resume        BOOLEAN,
    ADD COLUMN IF NOT EXISTS skills        TEXT[],
    ADD COLUMN IF NOT EXISTS source        TEXT,
    ADD COLUMN IF NOT EXISTS location      TEXT,
    ADD COLUMN IF NOT EXISTS position      TEXT,
    ADD COLUMN IF NOT EXISTS cover_letter  TEXT,
    ADD COLUMN IF NOT EXISTS date_of_birth DATE,
    ADD COLUMN IF NOT EXISTS phone_numbers TEXT[],
    ADD COLUMN IF NOT EXISTS created_at    TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS updated_at    TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS created_by    JSONB,
    ADD COLUMN IF NOT EXISTS applications  JSONB;

COMMENT ON COLUMN peopleforce_dm.recruitment_candidate.payload IS
  'Полный объект API; нормализованные копии — в именованных колонках.';

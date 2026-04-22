-- Нормализованные поля из типичных JSON API (создаётся поверх 010/011).

-- department_level
ALTER TABLE peopleforce_dm.department_level
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS updated_at   TIMESTAMPTZ;

-- team (вложенный team_lead + метки времени)
ALTER TABLE peopleforce_dm.team
    ADD COLUMN IF NOT EXISTS team_lead_id        BIGINT,
    ADD COLUMN IF NOT EXISTS team_lead_email     TEXT,
    ADD COLUMN IF NOT EXISTS team_lead_full_name TEXT,
    ADD COLUMN IF NOT EXISTS created_at          TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS updated_at          TIMESTAMPTZ;

-- leave_type
ALTER TABLE peopleforce_dm.leave_type
    ADD COLUMN IF NOT EXISTS unit       TEXT,
    ADD COLUMN IF NOT EXISTS fa_class     TEXT,
    ADD COLUMN IF NOT EXISTS hex_color    TEXT,
    ADD COLUMN IF NOT EXISTS created_at   TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS updated_at   TIMESTAMPTZ;

-- праздники
ALTER TABLE peopleforce_dm.public_holiday
    ADD COLUMN IF NOT EXISTS country_code TEXT,
    ADD COLUMN IF NOT EXISTS created_at   TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS updated_at   TIMESTAMPTZ;

ALTER TABLE peopleforce_dm.company_holiday
    ADD COLUMN IF NOT EXISTS created_at   TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS updated_at   TIMESTAMPTZ;

-- графики
ALTER TABLE peopleforce_dm.work_schedule
    ADD COLUMN IF NOT EXISTS created_at   TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS updated_at   TIMESTAMPTZ;

ALTER TABLE peopleforce_dm.working_pattern
    ADD COLUMN IF NOT EXISTS created_at   TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS updated_at   TIMESTAMPTZ;

ALTER TABLE peopleforce_dm.shift
    ADD COLUMN IF NOT EXISTS created_at   TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS updated_at   TIMESTAMPTZ;

-- time_entry
ALTER TABLE peopleforce_dm.time_entry
    ADD COLUMN IF NOT EXISTS created_at   TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS updated_at   TIMESTAMPTZ;

-- вакансия
ALTER TABLE peopleforce_dm.recruitment_vacancy
    ADD COLUMN IF NOT EXISTS title         TEXT,
    ADD COLUMN IF NOT EXISTS description   TEXT,
    ADD COLUMN IF NOT EXISTS tags          TEXT[],
    ADD COLUMN IF NOT EXISTS skills        TEXT[],
    ADD COLUMN IF NOT EXISTS created_at    TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS updated_at    TIMESTAMPTZ;

-- заявка на вакансию
ALTER TABLE peopleforce_dm.recruitment_application
    ADD COLUMN IF NOT EXISTS created_at            TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS updated_at            TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS pipeline_state_id     BIGINT,
    ADD COLUMN IF NOT EXISTS pipeline_state_name   TEXT,
    ADD COLUMN IF NOT EXISTS disqualified_at       TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS disqualify_reason_name TEXT;

-- pay_schedule
ALTER TABLE peopleforce_dm.pay_schedule
    ADD COLUMN IF NOT EXISTS created_at   TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS updated_at   TIMESTAMPTZ;

-- custom fields
ALTER TABLE peopleforce_dm.employee_custom_field
    ADD COLUMN IF NOT EXISTS field_position   INT,
    ADD COLUMN IF NOT EXISTS required         BOOLEAN,
    ADD COLUMN IF NOT EXISTS created_at        TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS updated_at        TIMESTAMPTZ;

-- прочие справочники
ALTER TABLE peopleforce_dm.asset
    ADD COLUMN IF NOT EXISTS serial_number  TEXT,
    ADD COLUMN IF NOT EXISTS asset_type     TEXT,
    ADD COLUMN IF NOT EXISTS status         TEXT,
    ADD COLUMN IF NOT EXISTS created_at     TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS updated_at     TIMESTAMPTZ;

ALTER TABLE peopleforce_dm.document_type
    ADD COLUMN IF NOT EXISTS category      TEXT,
    ADD COLUMN IF NOT EXISTS created_at    TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS updated_at    TIMESTAMPTZ;

ALTER TABLE peopleforce_dm.cost_center
    ADD COLUMN IF NOT EXISTS parent_id     BIGINT,
    ADD COLUMN IF NOT EXISTS created_at    TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS updated_at    TIMESTAMPTZ;

ALTER TABLE peopleforce_dm.project
    ADD COLUMN IF NOT EXISTS code          TEXT,
    ADD COLUMN IF NOT EXISTS status        TEXT,
    ADD COLUMN IF NOT EXISTS created_at    TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS updated_at    TIMESTAMPTZ;

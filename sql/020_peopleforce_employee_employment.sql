-- Статус трудоустройства и даты увольнения из PeopleForce API (GET/webhook employee).

ALTER TABLE peopleforce_dm.employee
    ADD COLUMN IF NOT EXISTS employment_status          TEXT,
    ADD COLUMN IF NOT EXISTS termination_effective_on   DATE,
    ADD COLUMN IF NOT EXISTS terminated_on             DATE;

COMMENT ON COLUMN peopleforce_dm.employee.employment_status IS 'PeopleForce: поле status (employed, terminated, probation, …)';
COMMENT ON COLUMN peopleforce_dm.employee.termination_effective_on IS 'PeopleForce: termination_effective_on при наличии в API';
COMMENT ON COLUMN peopleforce_dm.employee.terminated_on IS 'PeopleForce: terminated_on при наличии в API';

-- Расширение представления из 015: поля PF для сверки с HR master.
CREATE OR REPLACE VIEW master.v_hr_with_peopleforce_employee AS
SELECT
    h.*,
    e.full_name                  AS pf_dm_full_name,
    e.email                      AS pf_dm_email,
    e.employee_number            AS pf_dm_employee_number,
    e.employment_status          AS pf_dm_employment_status,
    e.termination_effective_on   AS pf_dm_termination_effective_on,
    e.terminated_on              AS pf_dm_terminated_on
FROM master.hr_employee h
LEFT JOIN peopleforce_dm.employee e ON e.id = h.pf_id;

COMMENT ON VIEW master.v_hr_with_peopleforce_employee IS 'Связка master.hr_employee.pf_id → peopleforce_dm.employee.id (+ статус/увольнение PF)';

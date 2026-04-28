-- Сотрудники PF в витрине, но без строки в master по pf_id

CREATE OR REPLACE VIEW master.v_pf_employees_missing_from_hr AS
SELECT
    e.id,
    e.full_name,
    e.email,
    e.personal_email
FROM peopleforce_dm.employee e
WHERE NOT EXISTS (
    SELECT 1
    FROM master.hr_employee h
    WHERE h.pf_id = e.id
);

-- Пользователи Pipedrive без строки мастера с тем же email

CREATE OR REPLACE VIEW master.v_pipedrive_users_missing_from_hr AS
SELECT
    u.id,
    u.name,
    u.email
FROM pipedrive_dm.pipedrive_user u
WHERE u.email IS NOT NULL
  AND trim(u.email) <> ''
  AND NOT EXISTS (
    SELECT 1
    FROM master.hr_employee h
    WHERE lower(trim(h.email)) = lower(trim(u.email))
);

COMMENT ON VIEW master.v_pf_employees_missing_from_hr IS 'Выгрузка PF без строки в master.hr_employee по pf_id';
COMMENT ON VIEW master.v_pipedrive_users_missing_from_hr IS 'CRM users без HR-строки по email';

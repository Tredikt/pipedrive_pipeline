-- Статусы сверки HR-мастера с выгрузками PeopleForce / Pipedrive (и задел под Jira).
-- Значения: «найден» | «Не найдено» | «нет …» | «не проверено»

ALTER TABLE master.hr_employee
    ADD COLUMN IF NOT EXISTS sync_status_pf TEXT,
    ADD COLUMN IF NOT EXISTS sync_status_pipedrive TEXT,
    ADD COLUMN IF NOT EXISTS sync_status_jira TEXT;

COMMENT ON COLUMN master.hr_employee.sync_status_pf IS 'Сверка с peopleforce_dm по pf_id / email';
COMMENT ON COLUMN master.hr_employee.sync_status_pipedrive IS 'Сверка с pipedrive (user/person) по email';
COMMENT ON COLUMN master.hr_employee.sync_status_jira IS 'Сверка с Jira (пока «не проверено», если нет витрины Jira)';

CREATE SEQUENCE IF NOT EXISTS master.hr_master_id_seq AS BIGINT START WITH 1000000001 INCREMENT BY 1;

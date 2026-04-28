-- Удобное представление: HR ↔ PeopleForce DM по pf_id ↔ employee.id

CREATE OR REPLACE VIEW master.v_hr_with_peopleforce_employee AS
SELECT
    h.*,
    e.full_name AS pf_dm_full_name,
    e.email     AS pf_dm_email,
    e.employee_number AS pf_dm_employee_number
FROM master.hr_employee h
LEFT JOIN peopleforce_dm.employee e ON e.id = h.pf_id;

COMMENT ON VIEW master.v_hr_with_peopleforce_employee IS 'Связка master.hr_employee.pf_id → peopleforce_dm.employee.id';

-- Пересчёт sync_status_* для master.hr_employee по текущим витринам PF / PD и задел под Jira.

UPDATE master.hr_employee h
SET sync_status_pf = CASE
    WHEN h.pf_id IS NULL THEN 'нет pf_id'
    WHEN EXISTS (SELECT 1 FROM peopleforce_dm.employee e WHERE e.id = h.pf_id) THEN 'найден'
    ELSE 'Не найдено'
END;

UPDATE master.hr_employee h
SET sync_status_pipedrive = CASE
    WHEN h.pipedrive_user_id IS NOT NULL
         AND EXISTS (SELECT 1 FROM pipedrive_dm.pipedrive_user u WHERE u.id = h.pipedrive_user_id)
        THEN 'найден'
    WHEN h.pipedrive_person_id IS NOT NULL
         AND EXISTS (SELECT 1 FROM pipedrive_dm.person p WHERE p.id = h.pipedrive_person_id)
        THEN 'найден'
    WHEN h.email IS NOT NULL AND trim(h.email) <> ''
         AND EXISTS (
             SELECT 1 FROM pipedrive_dm.pipedrive_user u
             WHERE lower(trim(u.email)) = lower(trim(h.email))
         )
        THEN 'найден'
    WHEN h.email IS NOT NULL AND trim(h.email) <> ''
         AND EXISTS (
             SELECT 1 FROM pipedrive_dm.person p
             WHERE lower(trim(p.primary_email)) = lower(trim(h.email))
         )
        THEN 'найден'
    WHEN h.pipedrive_user_id IS NOT NULL THEN 'Не найдено'
    WHEN h.pipedrive_person_id IS NOT NULL THEN 'Не найдено'
    WHEN h.email IS NULL OR trim(h.email) = '' THEN 'нет email для сверки'
    ELSE 'Не найдено'
END;

UPDATE master.hr_employee h
SET sync_status_jira = CASE
    WHEN h.jira_id IS NULL OR trim(h.jira_id) = '' THEN 'нет jira_id'
    ELSE 'не проверено'
END;

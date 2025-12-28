SELECT
    DATE_FORMAT(date, '%Y-%m') AS period,
    COUNT(*) AS completed_tasks,
    COUNT(DISTINCT project) AS projects,
    SUM(CASE WHEN completed = 1 THEN 1 ELSE 0 END) AS completed_flag,
    COUNT(DISTINCT workspace_id) AS workspaces
FROM asana_items
WHERE date BETWEEN :start_date AND :end_date
GROUP BY period
ORDER BY period DESC
LIMIT :limit_rows;

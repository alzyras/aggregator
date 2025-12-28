SELECT
    DATE_FORMAT(start_time, '%Y-%m') AS period,
    COUNT(*) AS entry_count,
    SUM(duration_minutes) AS duration_minutes,
    COUNT(DISTINCT project_name) AS projects,
    COUNT(DISTINCT client_name) AS clients
FROM toggl_items
WHERE start_time BETWEEN :start_date AND :end_date
GROUP BY period
ORDER BY period DESC
LIMIT :limit_rows;

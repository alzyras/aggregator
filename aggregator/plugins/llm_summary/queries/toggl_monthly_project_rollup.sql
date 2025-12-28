SELECT
    DATE_FORMAT(start_time, '%Y-%m') AS period,
    COALESCE(project_name, client_name, 'Uncategorized') AS category,
    SUM(duration_minutes) AS minutes
FROM toggl_items
WHERE start_time BETWEEN :start_date AND :end_date
GROUP BY period, category
ORDER BY period DESC, minutes DESC
LIMIT :limit_rows;

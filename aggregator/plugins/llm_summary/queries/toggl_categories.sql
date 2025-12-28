SELECT
    COALESCE(project_name, client_name, 'Uncategorized') AS category,
    SUM(duration_minutes) AS total_minutes,
    COUNT(*) AS entries
FROM toggl_items
WHERE start_time BETWEEN :start_date AND :end_date
GROUP BY category
ORDER BY total_minutes DESC
LIMIT :limit_rows;

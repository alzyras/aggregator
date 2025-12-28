SELECT source, category, total_value
FROM (
    SELECT 'asana' AS source, project AS category, COUNT(*) AS total_value FROM asana_items WHERE date >= DATE_SUB(:end_date, INTERVAL 30 DAY) GROUP BY project
    UNION ALL
    SELECT 'toggl', COALESCE(project_name, client_name, 'Uncategorized'), SUM(duration_minutes) FROM toggl_items WHERE start_time >= DATE_SUB(:end_date, INTERVAL 30 DAY) GROUP BY COALESCE(project_name, client_name, 'Uncategorized')
    UNION ALL
    SELECT 'habitica', COALESCE(item_type, 'unknown'), COUNT(*) FROM habitica_items WHERE date_completed >= DATE_SUB(:end_date, INTERVAL 30 DAY) GROUP BY COALESCE(item_type, 'unknown')
    UNION ALL
    SELECT 'google_fit', data_type, COUNT(*) FROM google_fit_general WHERE timestamp >= DATE_SUB(:end_date, INTERVAL 30 DAY) GROUP BY data_type
) t
ORDER BY total_value DESC
LIMIT :limit_rows;

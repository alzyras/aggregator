SELECT period, SUM(total_value) AS total_value
FROM (
    SELECT DATE_FORMAT(date, '%x-%v') AS period, COUNT(*) AS total_value FROM asana_items WHERE date BETWEEN :start_date AND :end_date GROUP BY period
    UNION ALL
    SELECT DATE_FORMAT(start_time, '%x-%v') AS period, SUM(duration_minutes) AS total_value FROM toggl_items WHERE start_time BETWEEN :start_date AND :end_date GROUP BY period
    UNION ALL
    SELECT DATE_FORMAT(date_completed, '%x-%v') AS period, COUNT(*) AS total_value FROM habitica_items WHERE date_completed BETWEEN :start_date AND :end_date GROUP BY period
    UNION ALL
    SELECT DATE_FORMAT(timestamp, '%x-%v') AS period, SUM(steps) AS total_value FROM google_fit_steps WHERE timestamp BETWEEN :start_date AND :end_date GROUP BY period
) t
GROUP BY period
ORDER BY period DESC
LIMIT :limit_rows;

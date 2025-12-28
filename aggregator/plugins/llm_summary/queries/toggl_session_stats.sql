SELECT
    AVG(duration_minutes) AS avg_minutes,
    SUM(CASE WHEN duration_minutes >= 60 THEN duration_minutes ELSE 0 END) AS deep_minutes,
    SUM(CASE WHEN duration_minutes >= 60 THEN 1 ELSE 0 END) AS deep_sessions,
    AVG(day_sessions) AS sessions_per_day
FROM (
    SELECT DATE(start_time) AS day, COUNT(*) AS day_sessions, SUM(duration_minutes) AS day_minutes
    FROM toggl_items
    WHERE start_time BETWEEN :start_date AND :end_date
    GROUP BY day
) d
CROSS JOIN (
    SELECT 1
) dummy
JOIN (
    SELECT duration_minutes FROM toggl_items WHERE start_time BETWEEN :start_date AND :end_date
) t;

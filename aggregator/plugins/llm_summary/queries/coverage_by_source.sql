SELECT 'asana' AS source, COUNT(DISTINCT DATE(date)) AS days_present
FROM asana_items
WHERE date BETWEEN :start_date AND :end_date
UNION ALL
SELECT 'toggl', COUNT(DISTINCT DATE(start_time)) FROM toggl_items WHERE start_time BETWEEN :start_date AND :end_date
UNION ALL
SELECT 'habitica', COUNT(DISTINCT DATE(date_completed)) FROM habitica_items WHERE date_completed BETWEEN :start_date AND :end_date
UNION ALL
SELECT 'google_fit', COUNT(DISTINCT DATE(timestamp)) FROM google_fit_steps WHERE timestamp BETWEEN :start_date AND :end_date;

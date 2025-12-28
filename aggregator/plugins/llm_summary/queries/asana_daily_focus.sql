SELECT DATE(date) AS day, COUNT(*) AS value
FROM asana_items
WHERE date >= :start_date AND date < :end_date
  AND (
    LOWER(COALESCE(task_name,'')) LIKE ANY(:patterns)
    OR LOWER(COALESCE(task_description,'')) LIKE ANY(:patterns)
    OR LOWER(COALESCE(project,'')) LIKE ANY(:patterns)
  )
GROUP BY day
ORDER BY day DESC;

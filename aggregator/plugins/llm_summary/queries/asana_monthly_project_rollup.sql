SELECT
    DATE_FORMAT(date, '%Y-%m') AS period,
    project AS category,
    COUNT(*) AS completed_tasks
FROM asana_items
WHERE date BETWEEN :start_date AND :end_date
GROUP BY period, category
ORDER BY period DESC, completed_tasks DESC
LIMIT :limit_rows;

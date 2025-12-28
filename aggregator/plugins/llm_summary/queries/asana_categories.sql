SELECT
    project AS category,
    COUNT(*) AS items,
    SUM(CASE WHEN completed = 1 THEN 1 ELSE 0 END) AS completed_items
FROM asana_items
WHERE date BETWEEN :start_date AND :end_date
GROUP BY category
ORDER BY items DESC
LIMIT :limit_rows;

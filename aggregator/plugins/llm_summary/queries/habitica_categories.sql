SELECT
    COALESCE(item_type, 'unknown') AS category,
    COUNT(*) AS items
FROM habitica_items
WHERE date_completed BETWEEN :start_date AND :end_date
GROUP BY category
ORDER BY items DESC
LIMIT :limit_rows;

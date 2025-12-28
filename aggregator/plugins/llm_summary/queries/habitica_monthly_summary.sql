SELECT
    DATE_FORMAT(date_completed, '%Y-%m') AS period,
    COUNT(*) AS completed_items,
    COUNT(DISTINCT item_type) AS item_types
FROM habitica_items
WHERE date_completed BETWEEN :start_date AND :end_date
GROUP BY period
ORDER BY period DESC
LIMIT :limit_rows;

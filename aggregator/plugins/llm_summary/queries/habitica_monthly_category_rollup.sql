SELECT
    DATE_FORMAT(date_completed, '%Y-%m') AS period,
    COALESCE(item_type, 'unknown') AS category,
    COUNT(*) AS completions
FROM habitica_items
WHERE date_completed BETWEEN :start_date AND :end_date
GROUP BY period, category
ORDER BY period DESC, completions DESC
LIMIT :limit_rows;

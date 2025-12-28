SELECT
    DATE(date_completed) AS day,
    COUNT(*) AS completions
FROM habitica_items
WHERE date_completed BETWEEN :start_date AND :end_date
GROUP BY day
ORDER BY day DESC;

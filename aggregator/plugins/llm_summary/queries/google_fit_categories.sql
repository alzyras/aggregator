SELECT
    data_type AS category,
    COUNT(*) AS records,
    AVG(value) AS avg_value
FROM google_fit_general
WHERE timestamp BETWEEN :start_date AND :end_date
GROUP BY category
ORDER BY records DESC
LIMIT :limit_rows;

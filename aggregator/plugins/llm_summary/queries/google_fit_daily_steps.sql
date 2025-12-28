SELECT
    DATE(timestamp) AS day,
    SUM(steps) AS steps
FROM google_fit_steps
WHERE timestamp BETWEEN :start_date AND :end_date
GROUP BY day
ORDER BY day DESC;

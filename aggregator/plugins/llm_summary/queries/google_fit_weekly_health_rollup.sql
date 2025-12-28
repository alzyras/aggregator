SELECT
    DATE_FORMAT(timestamp, '%x-%v') AS period,
    AVG(heart_rate) AS avg_hr,
    SUM(steps) AS steps
FROM (
    SELECT timestamp, NULL AS heart_rate, steps FROM google_fit_steps WHERE timestamp BETWEEN :start_date AND :end_date
    UNION ALL
    SELECT timestamp, heart_rate, NULL FROM google_fit_heart WHERE timestamp BETWEEN :start_date AND :end_date
) t
GROUP BY period
ORDER BY period DESC
LIMIT :limit_rows;

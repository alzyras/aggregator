SELECT
    period,
    SUM(steps_total) AS steps_total,
    AVG(heart_avg) AS heart_avg,
    SUM(general_count) AS general_count
FROM (
    SELECT DATE_FORMAT(timestamp, '%Y-%m') AS period,
           SUM(steps) AS steps_total,
           NULL AS heart_avg,
           NULL AS general_count
    FROM google_fit_steps
    WHERE timestamp BETWEEN :start_date AND :end_date
    GROUP BY period
    UNION ALL
    SELECT DATE_FORMAT(timestamp, '%Y-%m') AS period,
           NULL,
           AVG(heart_rate) AS heart_avg,
           NULL
    FROM google_fit_heart
    WHERE timestamp BETWEEN :start_date AND :end_date
    GROUP BY period
    UNION ALL
    SELECT DATE_FORMAT(timestamp, '%Y-%m') AS period,
           NULL,
           NULL,
           COUNT(*) AS general_count
    FROM google_fit_general
    WHERE timestamp BETWEEN :start_date AND :end_date
    GROUP BY period
) t
GROUP BY period
ORDER BY period DESC
LIMIT :limit_rows;

SELECT
    DATE_FORMAT({{date_column}}, '%Y-%m') AS period,
    COUNT(*) AS total_count,
    SUM({{value_column}}) AS total_value
FROM {{table_name}}
WHERE {{date_column}} BETWEEN :start_date AND :end_date
GROUP BY period
ORDER BY period DESC
LIMIT :limit_rows;

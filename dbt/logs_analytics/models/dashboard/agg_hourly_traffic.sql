{{ config(materialized='table') }}

-- Hourly traffic aggregated by 2xx success vs 4xx/5xx errors
-- Powers: "Requests over time" stacked bar chart
SELECT
    DATE_TRUNC('hour', f.request_timestamp) AS hour,
    COUNT(*) AS total_requests,
    COUNT(CASE WHEN s.status_class = '2xx' THEN 1 END) AS success_2xx,
    COUNT(CASE WHEN s.is_error = true THEN 1 END) AS errors_4xx5xx
FROM {{ ref('fact_requests') }} f
JOIN {{ ref('dim_status') }} s ON f.status_sk = s.status_sk
GROUP BY 1
ORDER BY 1

{{ config(materialized='table') }}

-- Daily bandwidth (GB) with weekday/weekend flag
-- Powers: "Body bytes sent over time" area line chart
SELECT
    d.full_date,
    d.is_weekend,
    CASE WHEN d.is_weekend THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    SUM(f.body_bytes_sent) / (1024.0 * 1024 * 1024) AS total_gb,
    COUNT(*) AS request_count,
    AVG(f.body_bytes_sent) / 1024.0 AS avg_kb_per_req
FROM {{ ref('fact_requests') }} f
JOIN {{ ref('dim_date') }} d ON f.date_sk = d.date_sk
GROUP BY 1, 2, 3
ORDER BY 1

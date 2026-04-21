{{ config(materialized='table') }}

-- Top error codes with percentage breakdown
-- Powers: "Error status detail" horizontal progress bars
SELECT
    s.status_code,
    s.status_class,
    COUNT(*) AS error_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
FROM {{ ref('fact_requests') }} f
JOIN {{ ref('dim_status') }} s ON f.status_sk = s.status_sk
WHERE s.is_error = true
GROUP BY 1, 2
ORDER BY 3 DESC
LIMIT 10

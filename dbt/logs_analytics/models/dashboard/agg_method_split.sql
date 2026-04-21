{{ config(materialized='table') }}

-- HTTP method distribution
-- Powers: "HTTP method split" grouped bar chart
SELECT
    r.method,
    COUNT(*) AS request_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
FROM {{ ref('fact_requests') }} f
JOIN {{ ref('dim_request') }} r ON f.request_sk = r.request_sk
GROUP BY 1
ORDER BY 2 DESC

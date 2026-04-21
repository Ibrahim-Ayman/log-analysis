{{ config(materialized='table') }}

-- Status class counts for donut chart
-- Powers: "Status class breakdown" pie/donut chart
SELECT
    status_class,
    COUNT(*) AS request_count
FROM {{ ref('dim_status') }} s
JOIN {{ ref('fact_requests') }} f ON s.status_sk = f.status_sk
GROUP BY 1
ORDER BY 2 DESC

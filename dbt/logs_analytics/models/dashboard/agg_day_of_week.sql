{{ config(materialized='table') }}

-- Average requests per hour grouped by day of week
-- Powers: "Traffic by day of week" bar chart with weekend highlight
SELECT
    d.day_name,
    CASE DAYOFWEEK(d.full_date)
        WHEN 0 THEN 0  -- Monday
        WHEN 1 THEN 1 WHEN 2 THEN 2 WHEN 3 THEN 3 WHEN 4 THEN 4 WHEN 5 THEN 5
        ELSE 6         -- Sunday
    END AS day_order,
    d.is_weekend,
    COUNT(*) AS total_requests,
    -- avg req per hour of that day  
    ROUND(COUNT(*) / 24.0, 2) AS avg_req_per_hour
FROM {{ ref('fact_requests') }} f
JOIN {{ ref('dim_date') }} d ON f.date_sk = d.date_sk
GROUP BY 1, 2, 3
ORDER BY 2

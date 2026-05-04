
  
    
    

    create  table
      "nginx_analytics"."main"."agg_error_detail__dbt_tmp"
  
    as (
      

-- Top error codes with percentage breakdown
-- Powers: "Error status detail" horizontal progress bars
SELECT
    s.status_code,
    s.status_class,
    COUNT(*) AS error_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
FROM "nginx_analytics"."main"."fact_requests" f
JOIN "nginx_analytics"."main"."dim_status" s ON f.status_sk = s.status_sk
WHERE s.is_error = true
GROUP BY 1, 2
ORDER BY 3 DESC
LIMIT 10
    );
  
  
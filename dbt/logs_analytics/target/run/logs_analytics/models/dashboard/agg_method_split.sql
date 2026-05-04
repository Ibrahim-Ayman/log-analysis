
  
    
    

    create  table
      "nginx_analytics"."main"."agg_method_split__dbt_tmp"
  
    as (
      

-- HTTP method distribution
-- Powers: "HTTP method split" grouped bar chart
SELECT
    r.method,
    COUNT(*) AS request_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
FROM "nginx_analytics"."main"."fact_requests" f
JOIN "nginx_analytics"."main"."dim_request" r ON f.request_sk = r.request_sk
GROUP BY 1
ORDER BY 2 DESC
    );
  
  
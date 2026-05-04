

-- Top 8 endpoints by total requests
-- Powers: "Top 8 endpoints" horizontal bar chart
SELECT
    r.url,
    r.method,
    COUNT(*) AS request_count
FROM "nginx_analytics"."main"."fact_requests" f
JOIN "nginx_analytics"."main"."dim_request" r ON f.request_sk = r.request_sk
GROUP BY 1, 2
ORDER BY 3 DESC
LIMIT 8
{{ config(materialized='table') }}

-- Single-row KPI table for the 4 Big Number cards
-- Powers: Total Requests, Unique IPs, Error Rate, Avg Bytes/Req KPI cards
SELECT
    COUNT(*)                                                          AS total_requests,
    COUNT(DISTINCT f.ip_sk)                                           AS unique_ips,
    ROUND(COUNT(CASE WHEN s.is_error THEN 1 END) * 100.0 / COUNT(*), 2) AS error_rate_pct,
    ROUND(AVG(f.body_bytes_sent) / 1024.0, 2)                        AS avg_kb_per_req
FROM {{ ref('fact_requests') }} f
JOIN {{ ref('dim_status') }} s ON f.status_sk = s.status_sk

WITH raw_dates AS (
    SELECT DISTINCT request_date as d
    FROM {{ ref('stg_nginx_logs') }}
)
SELECT
    md5(d::varchar) as date_sk,
    d as full_date,
    EXTRACT(year FROM d) as year,
    EXTRACT(month FROM d) as month,
    EXTRACT(day FROM d) as day,
    DAYNAME(d) as day_name,
    CASE WHEN DAYOFWEEK(d) IN (0, 6) THEN true ELSE false END as is_weekend
FROM raw_dates

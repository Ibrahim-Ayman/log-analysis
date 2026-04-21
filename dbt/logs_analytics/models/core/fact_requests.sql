SELECT
    md5(remote_addr || request_timestamp::varchar || method || url) as request_id, 
    md5(remote_addr) as ip_sk,
    md5(request_date::varchar) as date_sk,
    md5(status::varchar) as status_sk,
    md5(method || url || protocol) as request_sk,
    request_timestamp,
    body_bytes_sent,
    http_referer,
    user_agent
FROM {{ ref('stg_nginx_logs') }}

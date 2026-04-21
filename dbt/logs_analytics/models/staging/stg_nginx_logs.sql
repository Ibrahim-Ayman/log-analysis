SELECT
    md5(remote_addr || timestamp::varchar || method || url) as request_id,
    remote_addr,
    client_hostname,
    timestamp as request_timestamp,
    CAST(timestamp AS DATE) as request_date,
    method,
    url,
    protocol,
    status,
    status_class,
    is_error,
    body_bytes_sent,
    http_referer,
    user_agent
FROM nginx_analytics.main.nginx_silver_view

SELECT DISTINCT
    md5(method || url || protocol) as request_sk,
    method,
    url,
    protocol
FROM {{ ref('stg_nginx_logs') }}

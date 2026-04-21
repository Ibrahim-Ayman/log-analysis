SELECT DISTINCT
    md5(remote_addr) as ip_sk,
    remote_addr as ip_address,
    client_hostname
FROM {{ ref('stg_nginx_logs') }}

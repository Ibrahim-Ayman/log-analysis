SELECT DISTINCT
    md5(status::varchar) as status_sk,
    status as status_code,
    status_class,
    is_error
FROM "nginx_analytics"."main"."stg_nginx_logs"
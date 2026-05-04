
  
    
    

    create  table
      "nginx_analytics"."main"."dim_request__dbt_tmp"
  
    as (
      SELECT DISTINCT
    md5(method || url || protocol) as request_sk,
    method,
    url,
    protocol
FROM "nginx_analytics"."main"."stg_nginx_logs"
    );
  
  
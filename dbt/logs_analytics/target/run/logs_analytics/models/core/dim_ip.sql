
  
    
    

    create  table
      "nginx_analytics"."main"."dim_ip__dbt_tmp"
  
    as (
      SELECT DISTINCT
    md5(remote_addr) as ip_sk,
    remote_addr as ip_address,
    client_hostname
FROM "nginx_analytics"."main"."stg_nginx_logs"
    );
  
  
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select status_sk
from "nginx_analytics"."main"."dim_status"
where status_sk is null



      
    ) dbt_internal_test
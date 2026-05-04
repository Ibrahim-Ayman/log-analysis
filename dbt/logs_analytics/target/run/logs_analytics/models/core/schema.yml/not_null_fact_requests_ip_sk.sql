select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select ip_sk
from "nginx_analytics"."main"."fact_requests"
where ip_sk is null



      
    ) dbt_internal_test
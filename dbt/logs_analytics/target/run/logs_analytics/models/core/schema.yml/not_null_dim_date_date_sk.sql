select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select date_sk
from "nginx_analytics"."main"."dim_date"
where date_sk is null



      
    ) dbt_internal_test
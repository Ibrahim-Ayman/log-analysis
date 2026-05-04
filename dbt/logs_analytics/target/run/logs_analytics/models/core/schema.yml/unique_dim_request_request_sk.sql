select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    request_sk as unique_field,
    count(*) as n_records

from "nginx_analytics"."main"."dim_request"
where request_sk is not null
group by request_sk
having count(*) > 1



      
    ) dbt_internal_test
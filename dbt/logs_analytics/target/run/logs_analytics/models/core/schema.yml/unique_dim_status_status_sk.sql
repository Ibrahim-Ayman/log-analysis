select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    status_sk as unique_field,
    count(*) as n_records

from "nginx_analytics"."main"."dim_status"
where status_sk is not null
group by status_sk
having count(*) > 1



      
    ) dbt_internal_test
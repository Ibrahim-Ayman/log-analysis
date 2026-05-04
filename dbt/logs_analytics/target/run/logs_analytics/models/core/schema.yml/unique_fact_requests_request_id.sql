select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    request_id as unique_field,
    count(*) as n_records

from "nginx_analytics"."main"."fact_requests"
where request_id is not null
group by request_id
having count(*) > 1



      
    ) dbt_internal_test
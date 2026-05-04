select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with child as (
    select request_sk as from_field
    from "nginx_analytics"."main"."fact_requests"
    where request_sk is not null
),

parent as (
    select request_sk as to_field
    from "nginx_analytics"."main"."dim_request"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



      
    ) dbt_internal_test
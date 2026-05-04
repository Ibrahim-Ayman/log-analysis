
    
    

select
    status_sk as unique_field,
    count(*) as n_records

from "nginx_analytics"."main"."dim_status"
where status_sk is not null
group by status_sk
having count(*) > 1



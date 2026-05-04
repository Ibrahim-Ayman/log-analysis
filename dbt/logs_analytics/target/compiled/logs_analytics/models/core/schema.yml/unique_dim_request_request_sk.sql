
    
    

select
    request_sk as unique_field,
    count(*) as n_records

from "nginx_analytics"."main"."dim_request"
where request_sk is not null
group by request_sk
having count(*) > 1




    
    

select
    request_id as unique_field,
    count(*) as n_records

from "nginx_analytics"."main"."fact_requests"
where request_id is not null
group by request_id
having count(*) > 1



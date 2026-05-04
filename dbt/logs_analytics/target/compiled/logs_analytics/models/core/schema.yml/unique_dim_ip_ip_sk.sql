
    
    

select
    ip_sk as unique_field,
    count(*) as n_records

from "nginx_analytics"."main"."dim_ip"
where ip_sk is not null
group by ip_sk
having count(*) > 1



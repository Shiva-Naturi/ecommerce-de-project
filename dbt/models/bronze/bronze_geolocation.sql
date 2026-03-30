with source as (
    select * from {{ source('bronze', 'GEOLOCATION') }}
)
select * from source
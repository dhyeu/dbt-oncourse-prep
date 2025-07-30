{{ config(
    materialized = 'view',
    schema = 'analytics'
) }}

with source as (

    select * from {{ source('raw', 'raw_events') }}

),

renamed as (

    select
        event_id::string as event_id,
        customer_id::string as customer_id,
        event_type,
        event_timestamp::timestamp as event_time,
        device
    from source

)

select * from renamed

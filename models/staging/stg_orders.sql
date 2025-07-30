{{ config(
    materialized = 'view',
    schema = 'analytics'
) }}

with source as (

    select * from {{ source('raw', 'raw_orders') }}

),

renamed as (

    select
        order_id::string as order_id,
        customer_id::string as customer_id,
        order_date::date as order_date,
        order_amount::float as order_amount,
        payment_method
    from source

)

select * from renamed

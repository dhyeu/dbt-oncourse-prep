{{ config(
    materialized = 'view',
    schema = 'analytics'
) }}

with source as (

    select * from {{ source('raw', 'raw_customers') }}

),

renamed as (

    select
        customer_id::string as customer_id,
        concat(first_name, ' ', last_name) as customer_name,
        signup_date::date as signup_date,
        email,
        country
    from source

)

select * from renamed
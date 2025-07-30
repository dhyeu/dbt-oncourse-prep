{{ config(
    materialized = 'view',
    schema = 'dbt_dpatel_analytics'
) }}

with source as (

    select * from {{ source('raw', 'raw_customers') }}

),

renamed as (

    select
        customer_id,
        concat(first_name, ' ', last_name) as customer_name,
        lower(trim(email)) as email,
        signup_date::date as signup_date,
        country
    from source

)

select * from renamed
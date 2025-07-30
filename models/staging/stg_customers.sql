with source as (

    select * from {{ source('raw', 'raw_customers') }}

),

renamed as (

    select
        customer_id::string as customer_id,
        name as customer_name,
        signup_date::date as signup_date
    from source

)

select * from renamed

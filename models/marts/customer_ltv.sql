{{ config(
    materialized = 'table',
    schema = 'analytics'
) }}

with orders as (

    select * from {{ ref('stg_orders') }}

),

customers as (

    select * from {{ ref('stg_customers') }}

),

events as (

    select * from {{ ref('stg_events') }}

),

customer_agg as (

    select
        c.customer_id,
        c.customer_name,
        c.email,
        c.country,
        c.signup_date,
        count(distinct o.order_id) as total_orders,
        coalesce(sum(o.order_amount), 0) as lifetime_value,
        min(o.order_date) as first_order_date,
        max(o.order_date) as most_recent_order,
        count(e.event_id) as total_events,
        count(distinct e.event_type) as unique_event_types,
        max(e.event_time) as last_event_time
    from customers c
    left join orders o on c.customer_id = o.customer_id
    left join events e on c.customer_id = e.customer_id
    group by 1, 2, 3, 4, 5

)

select * from customer_agg
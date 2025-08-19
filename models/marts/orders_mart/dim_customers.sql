-- Builds a dimension table (materialized as a table in ORDERS_DB.ORDERS_MART).
-- Deduplicates by customer_id (keeps latest by signup_date).
-- Normalizes casing for names, lowercases email.
-- Adds a business key like CUST000001.

{{ config(materialized='table') }}

with src as (
    select *
    from {{ ref('stg_customers') }}
),

-- guardrail: if upstream ever sends duplicates for the same customer_id,
-- keep the latest by signup_date
dedup as (
    select
        customer_id::number                           as customer_id,
        first_name::varchar                            as first_name,
        last_name::varchar                             as last_name,
        lower(email)::varchar                          as email,
        signup_date::date                              as signup_date,
        row_number() over (
            partition by customer_id
            order by signup_date desc
        ) as rn
    from src
)

select
    customer_id,
    -- business-friendly surrogate key for BI tools / joins
    'CUST' || lpad(to_varchar(customer_id), 6, '0')   as customer_key,
    initcap(first_name)                                as first_name,
    initcap(last_name)                                 as last_name,
    initcap(concat(first_name, ' ', last_name))        as full_name,
    email,
    signup_date
from dedup
qualify rn = 1

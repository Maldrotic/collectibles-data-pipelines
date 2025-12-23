{{
  config(
    materialized='table',
    schema='marts',
    unique_key='order_id'
  )
}}

with events as (
  select * from {{ ref('int_events_combined') }}
),

filtered_events as (
  select
    *
  from events
  where source_system = 'orders'
    and event_type = 'OrderCreated'
),

dim_card as (
  select
    card_key,
    sku
  from {{ ref('dim_card') }}
),

final as (
  select
    payload:order_id::varchar as order_id,
    payload:customer_id::varchar as customer_id,
    dim_card.card_key as card_key,
    dim_card.sku as card_sku,
    payload:quantity::integer as quantity,
    payload:price:units::integer as price_cents,
    payload:status::varchar as status,
    payload:order_ts::timestamp_ntz as ordered_at,
    inserted_at
  from filtered_events
  left join dim_card on dim_card.sku = filtered_events.sku
)

select * from final
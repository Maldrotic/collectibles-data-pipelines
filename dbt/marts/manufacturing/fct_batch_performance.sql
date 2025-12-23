{{
  config(
    materialized='table',
    schema='marts',
    unique_key='batch_id'
  )
}}

with events as (
  select * from {{ ref('int_events_combined') }}
),

filtered_events as (
  select
    *
  from events
  where source_system = 'manufacturing'
    and event_type = 'ManufacturingBatch'
),

dim_card as (
  select
    card_key,
    sku
  from {{ ref('dim_card') }}
),

final as (
  select
    payload:batch_id::varchar as batch_id,
    dim_card.card_key as card_key,
    dim_card.sku as card_sku,
    payload:quantity_produced::integer as quantity_produced,
    payload:defect_rate_estimate:units::integer * pow(10, -payload:defect_rate_estimate:scale::integer)::decimal as defect_rate_estimate,
    (payload:quantity_produced::integer * (payload:defect_rate_estimate:units::integer * pow(10, -payload:defect_rate_estimate:scale::integer)::decimal))::integer as estimated_defect_count,
    payload:production_date::date as production_date,
    inserted_at
  from filtered_events
  left join dim_card on dim_card.sku = filtered_events.sku
)

select * from final
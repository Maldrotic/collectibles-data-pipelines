{{
  config(
    materialized='table',
    schema='marts',
    unique_key='sku'
  )
}}

with events as (
  select * from {{ ref('int_events_combined') }}
),

filtered_events as (
  select
    *
  from events
  where source_system = 'cards'
    and event_type = 'CardCatalogEntryInserted'
),

final as (
  select
    UUID_STRING() as card_key,
    payload:sku::varchar as sku,
    payload:card_name::varchar as card_name,
    payload:sport::varchar as sport,
    payload:player_name::varchar as player_name,
    payload:team_name::varchar as team_name,
    payload:parallel::varchar as parallel,
    payload:print_run::integer as print_run,
    inserted_at
  from filtered_events
)

select * from final

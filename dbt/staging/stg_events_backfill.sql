{{
  config(
    materialized='view',
    schema='staging'
  )
}}

with source as (
  select
    *
  from {{ source('raw_backfill', 'events_backfill')}}
),

renamed as (
  select
    id as event_id,
    source as event_source,
    type as event_type,
    payload as event_payload,
    inserted_at
  from source
)

select * from renamed
{{
  config(
    materialized='view'
  )
}}

with backfill_events as (
  select * from {{ ref('stg_events_backfill') }}
),

stream_events as (
  select * from {{ ref('stg_events_stream') }}
),

final as (
  select * from backfill_events
  union all
  select * from stream_events
)

select * from final
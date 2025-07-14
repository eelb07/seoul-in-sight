{{ config(
    materialized='incremental',
    unique_key='fact_event_id',
    incremental_strategy='merge',
    schema='fact'
) }}
with base as (
  select
    *,
    {{ dbt_utils.generate_surrogate_key([
       'event_name',
       "to_date(split_part(event_period,'~',1),'YYYY-MM-DD')",
       "to_date(split_part(event_period,'~',2),'YYYY-MM-DD')",
       'event_place',
       'longitude',
       'latitude',
       "case when is_paid then 'true' else 'false' end",
       'thumbnail_url',
       'event_url',
       'event_extra_detail'
    ]) }} as event_id_key
  from {{ ref('stg_event') }} b
  {% if is_incremental() %}
   where b.created_at > (select max(created_at) from {{ this }})
  {% endif %}
),
enriched as (
  select
    b.*,
    de.event_id,
    da.area_id
  from base b
  left join {{ ref('dim_event') }} de
    on de.event_id = b.event_id_key
  left join {{ source('dim_data','area') }} da
    on da.area_code = b.area_code
),

with_surrogates as (
  select
    enriched.*,
    {{ dbt_utils.generate_surrogate_key([
       'event_id',
       'observed_at'
    ]) }} as fact_event_id
  from enriched
  where event_id is not null
    and area_id  is not null
)

select
  fact_event_id,
  event_period,
  current_timestamp at time zone 'Asia/Seoul' as created_at,
  event_id,
  area_id,
  observed_at
from with_surrogates

{{ config(enabled=var('ad_reporting__facebook_ads_enabled')) }}

with adapter as (

    select *
    from {{ ref('facebook_ads__ad_adapter') }}

), aggregated as (

    select
        date_day,
        account_id,
        account_name,
        sum(clicks) as clicks,
        sum(impressions) as impressions,
        sum(spend) as spend,
        sum(conversions) as conversions
    from adapter
    {{ dbt_utils.group_by(3) }}

)

select *
from aggregated
{{ config(enabled=var('ad_reporting__facebook_ads_enabled')) }}

with report as (

    select *
    from {{ var('basic_ad') }}

), accounts as (

    select *
    from {{ var('account_history') }}
    where is_most_recent_record = true

), ads as (

    select *
    from {{ var('ad_history') }}
    where is_most_recent_record = true

), ad_sets as (

    select *
    from {{ var('ad_set_history') }}
    where is_most_recent_record = true

), campaigns as (

    select *
    from {{ var('campaign_history') }}
    where is_most_recent_record = true

), conversions as (

   select *
   from {{ var('ad_conversion') }}
   where is_most_recent_record = true

), joined as (

    select
        report.date_day,
        accounts.account_id,
        accounts.account_name,
        campaigns.campaign_id,
        campaigns.campaign_name,
        ad_sets.ad_set_id,
        ad_sets.ad_set_name,
        ads.ad_id,
        ads.ad_name,
        null as creative_id,
        null as creative_name,
        null as base_url,
        null as url_host,
        null as url_path,
        null as utm_source,
        null as utm_medium,
        null as utm_campaign,
        null as utm_content,
        null as utm_term,
        sum(report.clicks) as clicks,
        sum(report.impressions) as impressions,
        sum(report.spend) as spend,
        count(conversions.action_type) as conversions
    from report
    left join ads 
        on cast(report.ad_id as {{ dbt_utils.type_bigint() }}) = cast(ads.ad_id as {{ dbt_utils.type_bigint() }})
    left join conversions 
        on cast(ads.ad_id as {{ dbt_utils.type_bigint() }}) = cast(conversions.ad_id as {{ dbt_utils.type_bigint() }})
    left join ad_sets
        on cast(ads.ad_set_id as {{ dbt_utils.type_bigint() }}) = cast(ad_sets.ad_set_id as {{ dbt_utils.type_bigint() }})
    left join campaigns
        on cast(ads.campaign_id as {{ dbt_utils.type_bigint() }}) = cast(campaigns.campaign_id as {{ dbt_utils.type_bigint() }})
    left join accounts
        on cast(report.account_id as {{ dbt_utils.type_bigint() }}) = cast(accounts.account_id as {{ dbt_utils.type_bigint() }})
    {{ dbt_utils.group_by(19) }}


)

select *
from joined
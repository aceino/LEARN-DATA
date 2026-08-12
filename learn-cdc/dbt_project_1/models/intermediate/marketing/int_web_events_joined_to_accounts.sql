-- int_web_events_joined_to_accounts.sql  ← verb = "joined" not "aggregated"
with
web_events as (
    select * from {{ ref('stg_parch_porch_web_events') }}
),

accounts as (
    select * from {{ ref('stg_parch_porch_accounts') }}
),

joined as (
    select
        w.web_event_id,
        w.account_id,
        a.account_name,
        w.channel,
        w.visited_at
    from web_events w
    left join accounts a on w.account_id = a.account_id
)

select * from joined
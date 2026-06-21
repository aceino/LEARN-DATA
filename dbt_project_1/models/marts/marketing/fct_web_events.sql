with
web_events as (
    select * from {{ ref('int_web_events_joined_to_accounts') }}
),

final as (
    select
        -- keys
        web_event_id,
        account_id,

        -- time
        visited_at,

        -- attributes
        channel

    from web_events
)

select * from final
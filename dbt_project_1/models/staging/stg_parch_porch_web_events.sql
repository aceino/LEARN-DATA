with
source as (
    select * from {{ source('parch_porch', 'web_events') }}
),

renamed as (
    select
        id          as web_event_id,
        account_id,
        occurred_at as visited_at,
        channel
    from source
)

select * from renamed
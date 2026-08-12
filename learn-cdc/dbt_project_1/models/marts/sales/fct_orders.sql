with
orders as (
    select * from {{ ref('int_orders_joined_to_accounts') }}
),

final as (
    select
        -- keys
        order_id,
        account_id,
        sales_reps_id,
        region_id,

        -- time
        ordered_at,

        -- measures (MetricFlow will aggregate these)
        standard_qty,
        gloss_qty,
        poster_qty,
        total_qty,
        standard_amt_usd,
        gloss_amt_usd,
        poster_amt_usd,
        total_amt_usd

    from orders
)

select * from final
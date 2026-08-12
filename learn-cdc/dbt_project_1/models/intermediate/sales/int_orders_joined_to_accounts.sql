with

orders as (
    select * from {{ ref('stg_parch_porch_orders') }}
),

accounts as (
    select * from {{ ref('stg_parch_porch_accounts') }}
),

sales_reps as (
    select * from {{ ref('stg_parch_porch_sales_reps') }}
),

region as (
    select * from {{ ref('stg_parch_porch_region') }}
),

joined as (
    select
        -- order info
        o.order_id,
        o.ordered_at,
        o.standard_qty,
        o.gloss_qty,
        o.poster_qty,
        o.total_qty,
        o.standard_amt_usd,
        o.gloss_amt_usd,
        o.poster_amt_usd,
        o.total_amt_usd,

        -- account info
        a.account_id,
        a.account_name,

        -- sales rep info (FIXED: added 's' to match staging model)
        s.sales_reps_id,
        s.sales_reps_name,

        -- region info
        r.region_id,
        r.region_name

    from orders o
    left join accounts a    on o.account_id = a.account_id
    left join sales_reps s  on a.sales_rep_id = s.sales_reps_id
    left join region r      on s.region_id = r.region_id
)

select * from joined
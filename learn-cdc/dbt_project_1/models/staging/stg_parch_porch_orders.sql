with
source as (
    select * from {{ source('parch_porch', 'orders') }}
),

renamed as (
    select
        id              as order_id,
        account_id,
        occurred_at     as ordered_at,

        -- quantities
        standard_qty,
        gloss_qty,
        poster_qty,
        total           as total_qty,

        -- revenue
        standard_amt_usd,
        gloss_amt_usd,
        poster_amt_usd,
        total_amt_usd

    from source
)

select * from renamed
with
accounts as (
    select * from {{ ref('int_orders_joined_to_accounts') }}
),

final as (
    select distinct
        -- keys
        account_id,
        sales_reps_id,
        region_id,

        -- attributes
        account_name,
        sales_reps_name,
        region_name

    from accounts
)

select * from final
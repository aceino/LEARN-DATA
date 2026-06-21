with
source as (
    select * from {{ source('parch_porch', 'sales_reps') }}
),

renamed as (
    select
        id as sales_reps_id,
        region_id,
        name as sales_reps_name

    from source
)

select * from renamed
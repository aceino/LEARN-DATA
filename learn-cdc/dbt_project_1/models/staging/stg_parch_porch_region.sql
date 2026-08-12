with
source as (
    select * from {{ source('parch_porch', 'region') }}
),

renamed as (
    select
        id as region_id,
        name as region_name

    from source
)

select * from renamed
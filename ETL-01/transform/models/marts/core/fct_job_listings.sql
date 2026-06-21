with

intermediate_listings as (
    -- Reference your intermediate model
    select * from {{ ref('int_job_listing_group_by_regoin') }}
),

final_fact_table as (
    select
        -- Metrics / Keys
        job_id,
        
        -- Dimensions for filtering in dashboards
        job_title,
        company_name,
        standardized_region as job_region,
 
       -- Add a helper counter metric for easy aggregations in BI tools
        1 as listing_count

    from intermediate_listings
)

select * from final_fact_table
with 

staging_jobs as ( 
    select * from {{ ref( 'stg_adzuna__jobs')}}
),

standardized_location as ( 
    select 
        job_id,
        job_title, 
        company_name, 
        job_field, 
        location_raw, 

    -- Business Logic: standardized location data into clean region
    case 
        when location_raw like '%Manchester%' then 'Greater Manchester'
        when location_raw like '%London%' then 'Greater London'
        when location_raw in ('UK', 'United Kingdom') then 'UK-Wide'
        else location_raw 
    end as standardized_region

    from staging_jobs
)

select * from standardized_location
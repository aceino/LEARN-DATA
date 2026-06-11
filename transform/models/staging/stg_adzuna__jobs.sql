with 
source as ( 
    select * from {{source ('adzuna', 'raw_adzuna_jobs')}}
), 

renamed_and_cleaned as ( 
    select 
        id as job_id, 

        title as job_title,
        company as company_name, 
        category as job_field, 
        location as location_raw,

        created::timestamptz as created_at,
        date_trunc('day', created::timestamptz) as created_date
        
        from source
)

select * from renamed_and_cleaned


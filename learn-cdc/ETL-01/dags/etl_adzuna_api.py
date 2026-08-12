from airflow.sdk import dag, task, Variable 
import requests
import math 
import os
from pendulum import duration, datetime 
import time 

OBJECT_STORAGE_SYSTEM = os.getenv("OBJECT_STORAGE_SYSTEM", default="file")
OBJECT_STORAGE_PATH_JOBS = os.getenv("OBJECT_STORAGE_PATH_JOBS", default="include/jobs")
OBJECT_STORAGE_CONN_ID = os.getenv("OBJECT_STORAGE_CONN_ID", default=None)

MAX_PAGES = 10

@dag (
    start_date=datetime(2026, 6, 2),
    schedule=None,
    default_args = { 
        "retries" : 2 , 
        "retry_delay": duration(minutes=3),
    }
)

def personal_test():    
    @task
    def get_adzuna_api(): 

        ADZUNA_ID  = Variable.get("ADZUNA_ID")
        ADZUNA_KEY = Variable.get("ADZUNA_KEY")

        url = "https://api.adzuna.com/v1/api/jobs/gb/search/"
        base_params = { 
            'app_id': ADZUNA_ID,
            'app_key': ADZUNA_KEY, 
            'results_per_page': 50, 
            'what': "data engineer",
            'sort_by': "date",
        }
    
        job_list = []
        
        print("Fetching first page")
        
        response = requests.get(f"{url}1", params=base_params, timeout=10)

        if response.status_code != 200 : 
            error_message = f"Error fetching data from page 1 {response.status_code}, {response.text}"
            raise ValueError(error_message)
                   
        data = response.json() 
        total_result = data.get('count', 0)
        results_per_page = base_params['results_per_page']

        total_page = min(math.ceil(total_result / results_per_page), MAX_PAGES)
        
        print(f"Total results: {total_result}, Fetching {total_page} pages (capped at {MAX_PAGES})")
        job_list.extend(data.get('results', [])) 

        # Looping through the remaining 
        for page in range(2, total_page +1):
            time.sleep(1)
            try: 
                response = requests.get(f"{url}{page}", params=base_params, timeout=10)
                page_data = response.json()
                job_list.extend(page_data.get('results',[]))
            except Exception as e:
                print(f"Error fetching page{page}: {e}")
                continue 
        
        print(f"Total Jobs retrived: {len(job_list)}")
        
        return job_list

    @task(max_active_tis_per_dag=5)
    def job_adzuna(job_list: list[dict], **context:dict) -> None:
        import json 
        from airflow.sdk import ObjectStoragePath

        date = context['dag_run'].run_after.strftime("%Y-%m-%d")
      
        cleaned = []
        for job in job_list:
            cleaned.append({
                "id":          job.get('id'),
                "title":       job.get('title', '').strip(),
                "location":    job.get('location', {}).get('display_name', ''),
                "company":     job.get('company', {}).get('display_name', ''),
                "category":    job.get('category', {}).get('label', ''),
                "description": job.get('description', '')[:300],
                "url":         job.get('redirect_url', ''),
                "created":     job.get('created', ''),
            })

        object_storage_path = ObjectStoragePath( 
            f"{OBJECT_STORAGE_SYSTEM}://{OBJECT_STORAGE_PATH_JOBS}",
            conn_id = OBJECT_STORAGE_CONN_ID,
        )       
        
        path = object_storage_path / f"{date}_adzuna_jobs.json"
        path.write_text(json.dumps(cleaned, indent=2))
        print(f"Saved {len(cleaned)} jobs to {path}")

        return cleaned

    @task 
    def to_postgres(job_list): 
        import psycopg2        

        conn = psycopg2.connect( 
            host= Variable.get("POSTGRES_HOST"),
            port= Variable.get("POSTGRES_PORT"),
            dbname= Variable.get("POSTGRES_DB"),
            user= Variable.get("POSTGRES_USER"),
            password= Variable.get("POSTGRES_PASSWORD"),
        )

        cur = conn.cursor() 

        cur.execute(
            """
            create table if not exists raw_adzuna_jobs( 
            id Text Primary Key, 
            title text, 
            location text , 
            company text , 
            category text, 
            url text, 
            created text 
            )"""
        )

        for job in job_list:
            cur.execute("""
                INSERT INTO raw_adzuna_jobs 
                    (id, title, location, company, category, url, created)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """, (
                job.get('id'),
                job.get('title', '').strip(),
                job.get('location', {}).get('display_name', ''),
                job.get('company', {}).get('display_name', ''),
                job.get('category', {}).get('label', ''),
                        job.get('redirect_url', ''),
                job.get('created', ''),
            ))


        conn.commit()
        cur.close()
        conn.close()
        print(f"Inserted {len(job_list)} jobs into postgres")


    # Wire up: get_adzuna_api -> job_adzuna
    adzuna_data = get_adzuna_api()
    job_adzuna(adzuna_data)
    to_postgres(adzuna_data)

personal_test()
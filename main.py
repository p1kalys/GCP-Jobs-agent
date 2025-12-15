import os
import functions_framework
import requests
import gspread

PROJECT_ID = ""
TENANT_ID = "projects/{}/tenants/{}".format(PROJECT_ID, "default")

SERPAPI_API_KEY = ""
SERPAPI_URL = "https://serpapi.com/search"

MAX_JOBS_TO_FETCH = 500
RESULTS_PER_PAGE = 30

SPREADSHEET_ID = ""
TARGET_LOCATIONS = {
    "Europe": [
        {"filter_type": "location", "value": "United Kingdom"},
        {"filter_type": "location", "value": "Germany"},
        {"filter_type": "location", "value": "France"},
        {"filter_type": "location", "value": "Spain"},
        {"filter_type": "location", "value": "Italy"},
        {"filter_type": "location", "value": "Netherlands"},
        {"filter_type": "location", "value": "Poland"},
        {"filter_type": "location", "value": "Sweden"},
        {"filter_type": "location", "value": "Iceland"},
        {"filter_type": "location", "value": "Ireland"}
    ],
    "India": [
        {"filter_type": "location", "value": "India"} 
    ], 
    "US": [
        {"filter_type": "location", "value": "United States"}
    ]
}

SERVICE_ACCOUNT_KEY_FILE = "service_account_key.json"
COMPETITOR_NAMES = ["ITS"]

def filter_competitors(jobs_list: list) -> list:
    """Filters out jobs where the company name contains a competitor keyword."""
    competitor_keywords = [name.upper() for name in COMPETITOR_NAMES]
    filtered_jobs = []
    for job in jobs_list:
        company_name = job.get("company_name", "").upper()  
        is_competitor = False
        for keyword in competitor_keywords:
            if keyword in company_name:
                is_competitor = True
                print(f"--- FILTERED: Removed job '{job.get('title')}' at '{job.get('company_name')}' due to keyword '{keyword}'.")
                break
        if not is_competitor:
            filtered_jobs.append(job)
    print(f"\nFiltering complete. Kept {len(filtered_jobs)} jobs after removing competitors.")
    return filtered_jobs


def get_existing_job_ids(gc, worksheet_name) -> set:
    """Reads the 'job_id' column from the sheet for deduplication."""
    try:
        sheet = gc.open_by_key(SPREADSHEET_ID).worksheet(worksheet_name)
        job_id_column = sheet.col_values(1)[1:] 
        print(f"Read {len(job_id_column)} existing job IDs from the sheet.")
        return set(job_id_column)
    except Exception as e:
        print(f"Could not read existing job IDs. Starting with an empty set. Error: {e}")
        return set()


def write_jobs_to_sheet(gc, jobs_to_write: list, worksheet_name) -> int:
    """Appends new job records to the Google Sheet."""
    if not jobs_to_write:
        return 0
    try:
        sheet = gc.open_by_key(SPREADSHEET_ID).worksheet(worksheet_name)
        columns = ["job_id", "Title", "Company Name", "Source URL", "Location of Job", "Compensation", "Job Description", "Apply Link"]
        data_to_append = [
            [job.get(col, "") for col in columns] for job in jobs_to_write
        ]
        sheet.insert_rows(data_to_append, row=2, value_input_option='USER_ENTERED')
        return len(jobs_to_write)
    except Exception as e:
        print(f"Error writing to Google Sheets: {e}")
        raise


@functions_framework.http
def fetch_and_process_jobs(request):
    try:
        gc = gspread.service_account(filename=SERVICE_ACCOUNT_KEY_FILE)
    except Exception as e:
        return f"FATAL ERROR: Could not initialize Google Sheets client: {e}", 500
    
    for worksheet_name, filter_list in TARGET_LOCATIONS.items():
        aggregated_raw_job_data = []
        for filter_config in filter_list:
            filter_type = filter_config["filter_type"]
            filter_value = filter_config["value"]
            
            print(f"  > Fetching data for filter: {filter_value}")
            
            next_page_token = None
            
            while True:
                try:
                    params = {
                        "engine": "google_jobs",
                        "q": '"GCP" OR "Google Cloud Platform" OR "Google Cloud"',
                        "api_key": SERPAPI_API_KEY,
                        "hl": "en",
                        "num": RESULTS_PER_PAGE
                    }
                    
                    # Set the correct parameter based on the filter type
                    if filter_type == "gl":
                        params["gl"] = filter_value
                    elif filter_type == "location":
                        params["location"] = filter_value
                    
                    if next_page_token:
                        params["next_page_token"] = next_page_token
                        
                    response = requests.get(SERPAPI_URL, params=params)
                    response.raise_for_status()
                    data = response.json()
                    
                    if 'jobs_results' in data and data['jobs_results']:
                        for job in data['jobs_results']:
                            job_description = job.get("description", "No description provided")
                            aggregated_raw_job_data.append({
                                "job_id": job.get("job_id"),
                                "Title": job.get("title"),
                                "Company Name": job.get("company_name"),
                                "Location of Job": job.get("location", "N/A"),
                                "Compensation": job.get("detected_extensions", {}).get("salary", "N/A"),
                                "Source URL": job.get("share_link", "N/A"),
                                "Apply Link": job.get("apply_options", [{}])[0].get("link", job.get("share_link", "N/A")),
                                "Job Description": job_description,
                            })
                        
                        if len(aggregated_raw_job_data) >= MAX_JOBS_TO_FETCH:
                            print(f"Reached maximum job limit of {MAX_JOBS_TO_FETCH}. Exiting loop.")
                            break

                        pagination_data = data.get("serpapi_pagination", {})
                        next_page_token = pagination_data.get("next_page_token")
                        
                        if not next_page_token:
                            print("Pagination complete. No next page token found.")
                            break
                            
                    else:
                        print("No more results found in the latest fetch.")
                        break
                        
                except Exception as e:
                    return f"Error during Step 1 (Talent API Fetch) with token {next_page_token}: {e}", 500
                
        print(f"Step 1: Fetched {len(aggregated_raw_job_data)} raw job listings.")            
        
        filtered_jobs = filter_competitors(aggregated_raw_job_data)
        print(f"Step 2: Filtered down to {len(filtered_jobs)} non-competitor jobs.")
        
        existing_ids = get_existing_job_ids(gc, worksheet_name)
        new_jobs_to_add = []
        for job in filtered_jobs:
            if job.get("job_id") and job["job_id"] not in existing_ids:
                new_jobs_to_add.append(job)
        print(f"Deduplication: {len(new_jobs_to_add)} truly new jobs found for saving.")
        
        rows_written = 0
        if new_jobs_to_add:
            try:
                rows_written = write_jobs_to_sheet(gc, new_jobs_to_add, worksheet_name)
            except Exception as e:
                return f"Error during Step 3 (Google Sheets Write): {e}", 500

    return f"SUCCESS: Agent run complete. Fetched {len(aggregated_raw_job_data)} raw jobs, found {len(filtered_jobs)} non-competitors, and added {rows_written} new unique jobs to the sheet."
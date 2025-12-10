import os
import functions_framework
import requests
import gspread

PROJECT_ID = ""
TENANT_ID = "projects/{}/tenants/{}".format(PROJECT_ID, "default")

SERPAPI_API_KEY = ""
SERPAPI_URL = "https://serpapi.com/search"

SPREADSHEET_ID = ""
WORKSHEET_NAME = "GCP Job Listings"

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


def get_existing_job_ids(gc) -> set:
    """Reads the 'job_id' column from the sheet for deduplication."""
    try:
        sheet = gc.open_by_key(SPREADSHEET_ID).worksheet(WORKSHEET_NAME)
        job_id_column = sheet.col_values(1)[1:] 
        print(f"Read {len(job_id_column)} existing job IDs from the sheet.")
        return set(job_id_column)
    except Exception as e:
        print(f"Could not read existing job IDs. Starting with an empty set. Error: {e}")
        return set()


def write_jobs_to_sheet(gc, jobs_to_write: list) -> int:
    """Appends new job records to the Google Sheet."""
    if not jobs_to_write:
        return 0
    try:
        sheet = gc.open_by_key(SPREADSHEET_ID).worksheet(WORKSHEET_NAME)
        columns = ["job_id", "title", "company_name", "Source URL", "Location of Job", "Compensation", "Job Description", "Apply Link"]
        data_to_append = [
            [job.get(col, "") for col in columns] for job in jobs_to_write
        ]
        sheet.append_rows(data_to_append, value_input_option='USER_ENTERED')
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

    try:        
        params = {
            "engine": "google_jobs",
            "q": "GCP OR Google Cloud Platform",
            "api_key": SERPAPI_API_KEY,
            "hl": "en",
            "gl": "us"
        }
        response = requests.get(SERPAPI_URL, params=params)
        response.raise_for_status()
        data = response.json()
        raw_job_data = []
        if 'jobs_results' in data:
            for job in data['jobs_results']:
                raw_job_data.append({
                    "job_id": job.get("job_id"),
                    "title": job.get("title"),
                    "company_name": job.get("company_name"),
                    "Source URL": job.get("share_link"),
                    "Location of Job": job.get("location", "N/A"),
                    "Compensation": job.get("detected_extensions", {}).get("salary", "N/A"),
                    "Job Description": job.get("description", "No description provided"),
                    "Apply Link": job.get("apply_options", [{}])[0].get("link", job.get("share_link", "N/A")),
                })
        print(f"Step 1: Fetched {len(raw_job_data)} raw job listings.")            
    except Exception as e:
        return f"Error during Step 1 (Talent API Fetch): {e}", 500

    filtered_jobs = filter_competitors(raw_job_data)
    print(f"Step 2: Filtered down to {len(filtered_jobs)} non-competitor jobs.")
    print(f"Sample Job: {filtered_jobs[0]}")
    
    existing_ids = get_existing_job_ids(gc)
    new_jobs_to_add = []
    for job in filtered_jobs:
        if job.get("job_id") and job["job_id"] not in existing_ids:
            new_jobs_to_add.append(job)
    print(f"Deduplication: {len(new_jobs_to_add)} truly new jobs found for saving.")
    
    rows_written = 0
    if new_jobs_to_add:
        try:
            rows_written = write_jobs_to_sheet(gc, new_jobs_to_add)
        except Exception as e:
            return f"Error during Step 3 (Google Sheets Write): {e}", 500

    return f"SUCCESS: Agent run complete. Fetched {len(raw_job_data)} raw jobs, found {len(filtered_jobs)} non-competitors, and added {rows_written} new unique jobs to the sheet."
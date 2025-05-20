import csv
import json
import requests
import sys
import time
import os
from typing import List, Dict, Union, Optional

BRIGHT_DATA_API_TOKEN = "09aeea2d-50fc-42ec-9f2a-e3b8cfcc59be"
DATASET_ID = "gd_l1vikfnt1wgvvqz95w"
BASE_URL = "https://api.brightdata.com/datasets/v3"

def read_companies(csv_file: str) -> List[Dict[str, str]]:
    """Read company URLs from CSV file and format them for the API request."""
    companies = []
    with open(csv_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['company_url'].strip():  # Skip empty URLs
                companies.append({"url": row['company_url'].strip()})
    return companies

def trigger_collection(companies: List[Dict[str, str]]) -> Optional[str]:
    """
    Trigger a new data collection and return the snapshot ID.
    """
    url = f"{BASE_URL}/trigger"
    params = {
        "dataset_id": DATASET_ID,
        "include_errors": "true"
    }
    headers = {
        "Authorization": f"Bearer {BRIGHT_DATA_API_TOKEN}",
        "Content-Type": "application/json"
    }
    
    print("Triggering new data collection...")
    response = requests.post(url, headers=headers, params=params, json=companies)
    
    if response.status_code != 200:
        print(f"Error: Collection trigger failed with status code {response.status_code}")
        print(f"Response: {response.text}")
        return None
        
    try:
        result = response.json()
        snapshot_id = result.get('snapshot_id')
        if not snapshot_id:
            print("Error: No snapshot_id in response")
            return None
        print(f"Collection triggered successfully. Snapshot ID: {snapshot_id}")
        return snapshot_id
    except Exception as e:
        print(f"Error parsing response: {e}")
        return None

def check_progress(snapshot_id: str) -> str:
    """
    Check the progress of a collection. Returns status:
    - 'running': still gathering data
    - 'ready': data is available
    - 'failed': collection failed
    """
    url = f"{BASE_URL}/progress/{snapshot_id}"  # Include snapshot_id in the path
    headers = {"Authorization": f"Bearer {BRIGHT_DATA_API_TOKEN}"}
    
    try:
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            print(f"Error checking progress: {response.status_code}")
            print(f"Response: {response.text}")
            return 'failed'
            
        result = response.json()
        return result.get('status', 'failed')
    except Exception as e:
        print(f"Error checking progress: {e}")
        return 'failed'

def get_snapshots_list(status: str = 'ready', skip: int = 0, limit: int = 1000) -> Optional[List[Dict]]:
    """
    Get list of snapshots for the dataset.
    """
    url = f"{BASE_URL}/snapshots"
    params = {
        "dataset_id": DATASET_ID,
        "status": status,
        "skip": skip,
        "limit": limit
    }
    headers = {"Authorization": f"Bearer {BRIGHT_DATA_API_TOKEN}"}
    
    try:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code != 200:
            print(f"Error getting snapshots list: {response.status_code}")
            return None
        return response.json()
    except Exception as e:
        print(f"Error getting snapshots list: {e}")
        return None

def wait_for_completion(snapshot_id: str, timeout: int = 300, check_interval: int = 5) -> bool:
    """
    Wait for the collection to complete.
    Returns True if collection completed successfully, False otherwise.
    """
    start_time = time.time()
    print("\nWaiting for data collection to complete...")
    
    while time.time() - start_time < timeout:
        status = check_progress(snapshot_id)
        
        if status == 'ready':
            print("Data collection completed successfully!")
            return True
        elif status == 'failed':
            print("Data collection failed!")
            return False
        elif status == 'running':
            print(f"Status: {status}...")
            time.sleep(check_interval)
        else:
            print(f"Unknown status: {status}")
            return False
    
    print("Timeout reached while waiting for completion")
    return False

def fetch_results(snapshot_id: str, format: str = 'json') -> Optional[Union[List[Dict], Dict]]:
    """
    Fetch the results of a completed collection.
    """
    url = f"{BASE_URL}/snapshot/{snapshot_id}"  # Include snapshot_id in the path
    headers = {"Authorization": f"Bearer {BRIGHT_DATA_API_TOKEN}"}
    params = {"format": format}
    
    try:
        print("Fetching results...")
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code != 200:
            print(f"Error fetching results: {response.status_code}")
            print(f"Response: {response.text}")
            return None
            
        return response.json()
    except Exception as e:
        print(f"Error fetching results: {e}")
        return None

def save_results(data: Union[List[Dict], Dict], output_file: str):
    """Save the API response to a JSON file."""
    with open(output_file, 'w') as f:
        json.dump(data, f, indent=2)
    print(f"Results saved to {output_file}")

def display_company_stats(data: Union[List[Dict], Dict]):
    """Display basic statistics about the fetched company data."""
    print("\nCompany Data Summary:")
    print("-" * 80)
    
    # Handle both list and single company responses
    companies = data if isinstance(data, list) else [data]
    
    for company in companies:
        if isinstance(company, str):
            print(f"\nCompany URL: {company}")
            continue
            
        print(f"\nCompany: {company.get('name', 'N/A')}")
        print(f"Company ID: {company.get('company_id', 'N/A')}")
        print(f"Organization Type: {company.get('organization_type', 'N/A')}")
        print(f"Industry: {company.get('industries', 'N/A')}")
        print(f"Location: {company.get('headquarters', 'N/A')}")
        print(f"Company Size: {company.get('company_size', 'N/A')}")
        print(f"LinkedIn Followers: {company.get('followers', 'N/A')}")
        
        # Display website if available
        website = company.get('website')
        if website:
            print(f"Website: {website}")
        
        # Display about information if available
        about = company.get('about')
        if about:
            print("\nAbout:")
            print(about[:200] + "..." if len(about) > 200 else about)
        
        # Display recent updates if available
        updates = company.get('updates', [])
        if updates:
            print("\nRecent Updates:")
            for update in updates[:3]:  # Show last 3 updates
                date = update.get('date', 'N/A')
                text = update.get('text', 'N/A')
                likes = update.get('likes_count', 0)
                print(f"\n- Date: {date}")
                print(f"  Likes: {likes}")
                print(f"  Content: {text[:150]}..." if len(text) > 150 else f"  Content: {text}")
        
        print("-" * 80)

def main():
    input_file = "example_companies.csv"
    output_file = "company_data.json"
    
    print("Reading companies from CSV...")
    companies = read_companies(input_file)
    
    print(f"Processing {len(companies)} companies...")
    
    # Trigger collection and get snapshot ID
    snapshot_id = trigger_collection(companies)
    if not snapshot_id:
        print("Failed to trigger collection")
        sys.exit(1)
    
    # Wait for collection to complete
    if not wait_for_completion(snapshot_id):
        print("Collection failed or timed out")
        sys.exit(1)
    
    # Fetch and save results
    results = fetch_results(snapshot_id)
    if results:
        save_results(results, output_file)
        display_company_stats(results)
    else:
        print("Failed to fetch results")
        sys.exit(1)

if __name__ == "__main__":
    main() 
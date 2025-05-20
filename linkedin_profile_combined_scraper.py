import csv
import json
import requests
import sys
import time
import os
import datetime
import pandas as pd
import re
import argparse
import logging
from typing import List, Dict, Union, Optional, Tuple
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from apify_client import ApifyClient
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Bright Data API settings
BRIGHT_DATA_API_TOKEN = os.getenv('BRIGHT_DATA_API_TOKEN')  # Get from environment variable
COMPANY_DATASET_ID = os.getenv('BRIGHT_DATA_COMPANY_DATASET')  # Company scraper dataset ID
BRIGHT_DATA_BASE_URL = "https://api.brightdata.com/datasets/v3"

# Apify settings
APIFY_API_TOKEN = os.getenv('APIFY_TOKEN')  # Get from environment variable
LINKEDIN_PROFILE_SCRAPER_ACTOR_ID = "2SyF0bVxmgGr8IVCZ"  # LinkedIn Profile Scraper actor ID
LINKEDIN_COMPANY_SCRAPER_ACTOR_ID = "dev_fusion~linkedin-company-scraper"  # LinkedIn Company Scraper actor ID

# LinkedIn credentials (default values)
DEFAULT_LINKEDIN_USERNAME = os.getenv('LINKEDIN_USERNAME')
DEFAULT_LINKEDIN_PASSWORD = os.getenv('LINKEDIN_PASSWORD')

# Google API key and service account settings
API_KEY = os.getenv('GOOGLE_API_KEY')  # Get from environment variable
SERVICE_ACCOUNT_FILE = os.getenv('GOOGLE_SERVICE_ACCOUNT_FILE', "service-account.json")
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',  # Full access to spreadsheets
    'https://www.googleapis.com/auth/drive'           # Often needed for operations
]

def setup_logging():
    """Configure basic logging for the script"""
    logs_dir = create_directory("logs")
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(logs_dir, f"linkedin_profile_scraper_{timestamp}.log")
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    
    logger = logging.getLogger("linkedin_profile_scraper")
    logger.info(f"Logging initialized. Log file: {log_file}")
    return logger

def create_directory(dir_name):
    """Create a directory if it doesn't exist"""
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)
        logging.info(f"Created directory: {dir_name}")
    return dir_name

# --- Profile Data Fetcher Functions ---

def read_profiles(csv_file: str) -> List[Dict[str, str]]:
    """Read profile URLs from CSV file and format them for the API request."""
    profiles = []
    with open(csv_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['profileUrl'].strip():  # Skip empty URLs
                profiles.append(row['profileUrl'].strip())
    return profiles

def get_google_sheets_service():
    """Initialize and return Google Sheets service."""
    creds = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    return build('sheets', 'v4', credentials=creds)

def read_profiles_from_sheet(sheet_id: str, service, logger=None) -> List[str]:
    """Read LinkedIn profile URLs from Google Sheet's profileUrl column."""
    if logger is None:
        logger = logging.getLogger(__name__)
        
    try:
        # First get all values to find the profileUrl column
        result = service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range='A1:Z'  # Get all columns
        ).execute()
        
        values = result.get('values', [])
        if not values or len(values) < 2:  # Need at least header row and one data row
            logger.error('No data found in the sheet')
            return []
            
        # Find profileUrl column index
        headers = values[0]
        try:
            profile_url_col = headers.index('profileUrl')
            logger.info(f'Found profileUrl column at index {profile_url_col}')
        except ValueError:
            logger.error('Could not find profileUrl column in the sheet')
            return []
            
        # Extract profile URLs from the correct column
        profile_urls = []
        for row in values[1:]:  # Skip header row
            if len(row) > profile_url_col:
                url = row[profile_url_col].strip()
                if url and 'linkedin.com/in/' in url:
                    profile_urls.append(url)
                    
        if not profile_urls:
            logger.error('No valid LinkedIn profile URLs found in the profileUrl column')
            return []
            
        logger.info(f'Found {len(profile_urls)} valid LinkedIn profile URLs')
        return profile_urls
        
    except HttpError as err:
        logger.error(f'Error reading from Google Sheet: {err}')
        return []

def scrape_linkedin_profiles(profile_urls, apify_token, logger):
    """Scrape LinkedIn profiles using Apify"""
    logger.info(f"Starting Apify scraper for {len(profile_urls)} profiles")
        
    try:
        # Prepare the Apify input with correct parameter name
        input_data = {
            "profileUrls": profile_urls,  # Changed from linkedInProfileUrls to profileUrls
            "includeContactInfo": True,
                "includeActivityData": True,
                "includeEducationData": True,
            "includeExperienceData": True
        }
        
        # Create Apify client
        client = ApifyClient(apify_token)
        
        # Start the actor and wait for it to finish
        run = client.actor("2SyF0bVxmgGr8IVCZ").call(run_input=input_data)
        
        # Fetch results
        items = []
        for item in client.dataset(run["defaultDatasetId"]).iterate_items():
            items.append(item)
            
        logger.info(f"Successfully scraped {len(items)} profiles with Apify")
        return items
        
    except Exception as e:
        logger.error(f"Error in Apify scraping: {str(e)}")
        return None

def save_results(data, csv_file, json_file, logger):
    """Save results to both JSON and CSV files"""
    try:
        # Save raw data to JSON
        with open(json_file, 'w') as f:
            json.dump(data, f, indent=2)
        logger.info(f"Raw data saved to {json_file}")
        
        # Convert to DataFrame and save to CSV
        profiles_list = []
        for profile in data:
            profile_dict = {
                'fullName': profile.get('fullName', 'N/A'),
                'linkedinUrl': profile.get('linkedinUrl', 'N/A'),
                'headline': profile.get('headline', 'N/A'),
                'location': profile.get('addressWithCountry', 'N/A'),
                'connections': profile.get('connections', 'N/A'),
                'followers': profile.get('followers', 'N/A')
            }
            
            # Add current company info if available
            experiences = profile.get('experiences', [])
            if experiences:
                current_company = experiences[0]  # Most recent experience
                profile_dict.update({
                    'currentCompany': current_company.get('companyName', 'N/A'),
                    'companyUrl': current_company.get('companyLink1', 'N/A'),
                    'companyIndustry': current_company.get('industry', 'N/A'),
                    'jobTitle': current_company.get('title', 'N/A')
                })
            
            profiles_list.append(profile_dict)
        
        df = pd.DataFrame(profiles_list)
        df.to_csv(csv_file, index=False)
        logger.info(f"Processed data saved to {csv_file}")
        
    except Exception as e:
        logger.error(f"Error saving results: {str(e)}")
        return False
    
    return True

def format_company_url(url: str) -> str:
    """Format company URL to be compatible with the Bright Data API requirements.
    
    Args:
        url: LinkedIn company URL to format
        
    Returns:
        Formatted URL suitable for Bright Data API
    """
    if not url or url == 'N/A':
        return url
        
    # Remove tracking parameters and clean up the URL
    url = url.split('?')[0].strip('/')
    
    # Extract company name/id from URL
    if '/company/' in url:
        company_id = url.split('/company/')[-1].split('/')[0]
        # Format URL to match the working pattern
        return f"https://www.linkedin.com/company/{company_id}"
    
    # Handle cases where URL might be in a different format
    if 'linkedin.com' in url and not url.startswith('http'):
        return f"https://www.{url}"
    elif not url.startswith('http'):
        return f"https://www.linkedin.com/company/{url}"
    
    return url

def scrape_companies(company_urls: List[str], logger=None) -> Optional[List[Dict]]:
    """Scrape LinkedIn company data using Bright Data."""
    if logger is None:
        logger = logging.getLogger(__name__)
    
    try:
        # Format input for Bright Data API
        inputs = [{"url": url} for url in company_urls]
        
        # Trigger collection
        url = f"{BRIGHT_DATA_BASE_URL}/trigger"
        params = {
            "dataset_id": COMPANY_DATASET_ID,
            "include_errors": "true"
        }
        headers = {
            "Authorization": f"Bearer {BRIGHT_DATA_API_TOKEN}",
            "Content-Type": "application/json"
        }
        
        logger.info("Starting LinkedIn company scraping with Bright Data...")
        response = requests.post(url, headers=headers, params=params, json=inputs)
        
        if response.status_code != 200:
            logger.error(f"Error: Collection trigger failed with status code {response.status_code}")
            logger.error(f"Response: {response.text}")
            return None
            
        result = response.json()
        snapshot_id = result.get('snapshot_id')
        if not snapshot_id:
            logger.error("Error: No snapshot_id in response")
            return None
            
        logger.info(f"Collection triggered successfully. Snapshot ID: {snapshot_id}")
        
        # Wait for completion
        max_wait_time = 300  # 5 minutes timeout
        check_interval = 5  # Check every 5 seconds
        start_time = time.time()
        
        while time.time() - start_time < max_wait_time:
            # Check progress
            progress_url = f"{BRIGHT_DATA_BASE_URL}/progress/{snapshot_id}"
            progress_response = requests.get(progress_url, headers={"Authorization": f"Bearer {BRIGHT_DATA_API_TOKEN}"})
            
            if progress_response.status_code != 200:
                logger.error(f"Error checking progress: {progress_response.status_code}")
                logger.error(f"Response: {progress_response.text}")
                return None
                
            status = progress_response.json().get('status')
            
            if status == 'ready':
                logger.info("Data collection completed successfully!")
                break
            elif status == 'failed':
                logger.error("Data collection failed!")
                return None
            elif status == 'running':
                logger.info(f"Status: {status}...")
                time.sleep(check_interval)
            else:
                logger.error(f"Unknown status: {status}")
                return None
        else:
            logger.error("Timeout reached while waiting for completion")
            return None
        
        # Fetch results
        results_url = f"{BRIGHT_DATA_BASE_URL}/snapshot/{snapshot_id}"
        results_response = requests.get(
            results_url, 
            headers={"Authorization": f"Bearer {BRIGHT_DATA_API_TOKEN}"},
            params={"format": "json"}
        )
        
        if results_response.status_code != 200:
            logger.error(f"Error fetching results: {results_response.status_code}")
            logger.error(f"Response: {results_response.text}")
            return None
            
        results = results_response.json()
        logger.info(f"Successfully scraped {len(results) if isinstance(results, list) else 1} companies")
        return results
        
    except Exception as e:
        logger.error(f"Error during company scraping: {str(e)}")
        return None

def extract_company_details(profile_data: Union[List[Dict], Dict], logger=None) -> List[Dict]:
    """Extract company details from LinkedIn profiles and fetch additional data using Bright Data.
    
    Args:
        profile_data: Either a single profile dict or a list of profile dicts from Apify
        logger: Optional logger instance
        
    Returns:
        List of dictionaries containing combined profile and company data
    """
    if logger is None:
        logger = logging.getLogger(__name__)
        
    # Handle both list and single profile responses
    profiles = profile_data if isinstance(profile_data, list) else [profile_data]
    company_details = []
    company_urls_map = {}  # Map to track which profile each company URL belongs to
    
    # First pass: Collect all company URLs and initialize company details
    company_requests = []
    for profile in profiles:
        if isinstance(profile, str):
            continue
            
        # Get current company info from experiences
        experiences = profile.get('experiences', [])
        if experiences and len(experiences) > 0:
            current_company = experiences[0]  # First experience is current
            company_url = current_company.get('companyLink1', 'N/A')
            
            # Initialize company detail with profile info
            company_detail = {
                'profile_name': profile.get('fullName', 'N/A'),
                'profile_url': profile.get('linkedinUrl', 'N/A'),
                'profile_location': profile.get('addressWithCountry', 'N/A'),
                'profile_headline': profile.get('headline', 'N/A'),
                'profile_connections': profile.get('connections', 'N/A'),
                'profile_followers': profile.get('followers', 'N/A'),
                'company_name': current_company.get('subtitle', '').split('·')[0].strip(),
                'company_url': company_url,
                'job_title': current_company.get('title', 'N/A'),
                'job_duration': current_company.get('caption', 'N/A'),
                'snapshot_date': datetime.datetime.now().strftime("%Y-%m-%d"),
                'all_time_ads': 0,  # Initialize ad counts
                'last_30_days_ads': 0
            }
            
            company_details.append(company_detail)
            
            # If we have a valid company URL, add it to our batch request
            if company_url != 'N/A' and 'linkedin.com/company/' in company_url:
                formatted_url = format_company_url(company_url)
                if formatted_url != company_url:
                    logger.info(f"Reformatted company URL from {company_url} to {formatted_url}")
                
                company_requests.append({"url": formatted_url})
                company_urls_map[formatted_url] = len(company_details) - 1
    
    # If we have company URLs to process, make a batch request using Bright Data
    if company_requests:
        try:
            logger.info(f"Making batch request for {len(company_requests)} companies using Bright Data...")
            
            # Create new snapshot
            snapshot_id = trigger_collection(company_requests)
            if not snapshot_id:
                logger.error("Failed to create snapshot for company data")
                return company_details
                
            # Wait for snapshot to complete
            if not wait_for_completion(snapshot_id):
                logger.error("Snapshot failed or timed out")
                return company_details
                
            # Fetch results from snapshot
            company_data = fetch_results(snapshot_id)
            if not company_data:
                logger.error("Failed to fetch company data from snapshot")
                return company_details
                
            # Update company details with data from Bright Data
            for company in company_data:
                company_url = company.get('url')
                if company_url in company_urls_map:
                    idx = company_urls_map[company_url]
                    company_details[idx].update({
                        'company_id': company.get('company_id', 'N/A'),
                        'company_name': company.get('name', company_details[idx]['company_name']),
                        'company_industry': company.get('industries', 'N/A'),
                        'company_size': company.get('company_size', 'N/A'),
                        'company_followers': company.get('followers', 'N/A'),
                        'company_employees_on_linkedin': company.get('employees_in_linkedin', 'N/A'),
                        'company_headquarters': company.get('headquarters', 'N/A'),
                        'company_founded': company.get('founded', 'N/A'),
                        'company_website': company.get('website', 'N/A'),
                        'company_specialties': company.get('specialties', 'N/A'),
                        'company_type': company.get('organization_type', 'N/A'),
                        'company_about': company.get('about', 'N/A')
                    })
                    logger.info(f"Updated company details for {company.get('name', 'Unknown Company')}")
                    
        except Exception as e:
            logger.error(f"Error fetching company data: {str(e)}")

    # Now set up Chrome for ad scraping
    chrome_options = Options()
    chrome_options.add_argument('--headless=new')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--disable-software-rasterizer')
    
    try:
        # Initialize ChromeDriver with correct path handling
        driver_manager = ChromeDriverManager()
        driver_path = driver_manager.install()
        logger.info(f"Initial ChromeDriver path: {driver_path}")

        # Get the actual chromedriver executable path
        if 'chromedriver-mac-arm64' in driver_path:
            driver_path = os.path.join(os.path.dirname(driver_path), 'chromedriver')
        elif 'THIRD_PARTY_NOTICES' in driver_path:
            driver_path = os.path.join(os.path.dirname(driver_path), 'chromedriver')
            
        if not os.path.exists(driver_path):
            # Try to find chromedriver in the parent directory
            parent_dir = os.path.dirname(os.path.dirname(driver_path))
            potential_path = os.path.join(parent_dir, 'chromedriver')
            if os.path.exists(potential_path):
                driver_path = potential_path
            else:
                raise Exception(f"ChromeDriver executable not found in {driver_path} or {potential_path}")

        logger.info(f"Using ChromeDriver executable at: {driver_path}")
        os.chmod(driver_path, 0o755)  # Ensure executable permissions
        
        # Create browser instance
        service = Service(executable_path=driver_path)
        browser = webdriver.Chrome(service=service, options=chrome_options)
        browser.set_window_size(1920, 1080)
        
        try:
            # Login to LinkedIn
            if not login_to_linkedin(browser, args.linkedin_username, args.linkedin_password, logger=logger):
                logger.error("Failed to login to LinkedIn")
                return

            # Process each company
            for company in company_data:
                company_id = company.get('company_id')
                company_name = company.get('name', 'Unknown')
                
                if not company_id:
                    logger.warning(f"No company ID for {company_name}, skipping...")
                    continue
                
                logger.info(f"\nProcessing {company_name} (ID: {company_id})")
                
                # Get all-time ads count
                url = f"https://www.linkedin.com/ad-library/search?companyIds={company_id}"
                all_time_count = get_ads_count(browser, url, logger)
                    
                # Get last 30 days count
                url = f"https://www.linkedin.com/ad-library/search?companyIds={company_id}&dateOption=last-30-days"
                last_30_count = get_ads_count(browser, url, logger)
                
                # Update company data
                company['all_time_ads'] = all_time_count if all_time_count is not None else 0
                company['last_30_days_ads'] = last_30_count if last_30_count is not None else 0
                
                logger.info(f"Ad counts for {company_name}:")
                logger.info(f"  All time: {company['all_time_ads']}")
                logger.info(f"  Last 30 days: {company['last_30_days_ads']}")
                
                # Small delay between companies
                time.sleep(args.wait)
                
            # Save updated company data
            with open(args.company_data, 'w') as f:
                json.dump(company_data, f, indent=2)
            logger.info(f"\nUpdated company data saved to {args.company_data}")

            logger.info("Company data before final merge (URLs, names, and IDs):")
            if company_data:
                for i, comp_entry in enumerate(company_data):
                    c_url = comp_entry.get('url', 'N/A_URL_IN_COMP_DATA')
                    c_name = comp_entry.get('name', 'N/A_NAME_IN_COMP_DATA')
                    c_id = comp_entry.get('company_id', 'N/A_ID_IN_COMP_DATA')
                    # Also log the ad counts as they exist in company_data at this point
                    c_all_ads = comp_entry.get('all_time_ads', 'N/A_ALL_ADS')
                    c_30d_ads = comp_entry.get('last_30_days_ads', 'N/A_30D_ADS')
                    logger.info(f"  Entry {i}: Name='{c_name}', ID='{c_id}', URL='{c_url}', FormattedURL='{format_company_url(c_url)}', AllAds='{c_all_ads}', 30dAds='{c_30d_ads}'")
            else:
                logger.warning("  company_data list is empty or None before final merge.")
            
            # Create final combined data
            combined_data = []
            def extract_company_id_from_url(url):
                match = re.search(r"/company/(\d+)", str(url))
                return match.group(1) if match else None
            for profile in profile_data:
                profile_company_id = None
                raw_company_link = "N/A_RAW_LINK" # For logging
                if profile.get('experiences') and len(profile['experiences']) > 0:
                    raw_company_link = profile['experiences'][0].get('companyLink1', 'N/A_IN_EXPERIENCE')
                    profile_company_id = extract_company_id_from_url(raw_company_link)
                logger.info(f"Attempting to merge for profile: '{profile.get('fullName', 'N/A_PROFILE_NAME')}'")
                logger.info(f"  Raw companyLink1 from profile: '{raw_company_link}'")
                logger.info(f"  Extracted profile_company_id for matching: '{profile_company_id}'")
                company_info_for_profile = {} # Default to empty if no match
                if profile_company_id and profile_company_id != 'N/A': # Only attempt match if profile_company_id is valid
                    if not company_data:
                        logger.warning("  Company data list is empty or None during merge step. Cannot find a match.")
                    else:
                        match_found_in_company_data = False
                        for c_idx, company_entry in enumerate(company_data):
                            company_data_id = str(company_entry.get('company_id', ''))
                            logger.info(f"    Checking against company_data entry #{c_idx}: Name='{company_entry.get('name', 'N/A_COMPANY_NAME')}', ID='{company_data_id}'")
                            if company_data_id == str(profile_company_id):
                                company_info_for_profile = company_entry
                                logger.info(f"  SUCCESSFUL MATCH: Profile '{profile.get('fullName', 'N/A_PROFILE_NAME')}' (ID: '{profile_company_id}')")
                                logger.info(f"    Matched with company_data entry: Name='{company_entry.get('name', 'N/A_COMPANY_NAME')}', ID='{company_data_id}'")
                                match_found_in_company_data = True
                                break # Found a match, no need to check further for this profile
                        if not match_found_in_company_data:
                            logger.warning(f"  NO MATCH FOUND for profile '{profile.get('fullName', 'N/A_PROFILE_NAME')}' (ID: '{profile_company_id}') in {len(company_data)} company_data entries.")
                else:
                    logger.warning(f"  SKIPPING MATCH for profile '{profile.get('fullName', 'N/A_PROFILE_NAME')}' because its company ID is invalid or missing (Raw: '{raw_company_link}', Extracted: '{profile_company_id}').")
                company_info = company_info_for_profile # Use the matched company_info or the default empty {}
                combined_record = {
                    'profile_name': profile.get('fullName', 'N/A'),
                    'profile_url': profile.get('linkedinUrl', 'N/A'),
                    'profile_location': profile.get('addressWithCountry', 'N/A'),
                    'profile_headline': profile.get('headline', 'N/A'),
                    'profile_connections': profile.get('connections', 'N/A'),
                    'profile_followers': profile.get('followers', 'N/A'),
                    'company_name': company_info.get('name', 'N/A'),
                    'company_url': company_info.get('url', 'N/A'),
                    'company_id': company_info.get('company_id', 'N/A'),
                    'company_industry': company_info.get('industries', 'N/A'),
                    'company_size': company_info.get('company_size', 'N/A'),
                    'company_followers': company_info.get('followers', 'N/A'),
                    'all_time_ads': company_info.get('all_time_ads', 0),
                    'last_30_days_ads': company_info.get('last_30_days_ads', 0),
                    'linkedin_ads': 'y' if (company_info.get('all_time_ads', 0) > 0 or company_info.get('last_30_days_ads', 0) > 0) else 'n',
                    'snapshot_date': datetime.datetime.now().strftime("%Y-%m-%d")
                }
                combined_data.append(combined_record)
            # Save to CSV
            df = pd.DataFrame(combined_data)
            df.to_csv(args.output, index=False)
            logger.info(f"\nFinal combined results saved to {args.output}")

            # --- NEW: Update ONLY the ad columns in Google Sheet ---
            try:
                service = get_google_sheets_service()
                sheet = service.spreadsheets().get(spreadsheetId=args.sheet_id).execute()
                sheet_title = sheet['sheets'][0]['properties']['title']
                # Get existing data
                result = service.spreadsheets().values().get(
                    spreadsheetId=args.sheet_id,
                    range=f"{sheet_title}!A1:Z"
                ).execute()
                values = result.get('values', [])
                if not values or len(values) < 2:
                    logger.error('No data found in the sheet')
                    return
                headers = values[0]
                # Find column indices
                try:
                    profile_url_col = headers.index('profileUrl')
                    li_ads_col = headers.index('LI Ads?')
                    days_30_col = headers.index('30 days')
                    overall_col = headers.index('Overall')
                except ValueError as e:
                    logger.error(f"Required column missing: {e}")
                    return
                # Build a lookup from combined_data by profile_url
                combined_lookup = {row.get('profile_url', ''): row for row in combined_data}
                # Prepare batch update
                data = []
                for i, row in enumerate(values[1:], start=2):  # 1-based index, skip header
                    if len(row) > profile_url_col:
                        url = row[profile_url_col]
                        if url in combined_lookup:
                            cdata = combined_lookup[url]
                            li_ads = 'y' if (cdata.get('all_time_ads', 0) > 0 or cdata.get('last_30_days_ads', 0) > 0) else 'n'
                            days_30 = cdata.get('last_30_days_ads', 0)
                            overall = cdata.get('all_time_ads', 0)
                            # Prepare update for each column
                            if len(row) <= li_ads_col:
                                row += [''] * (li_ads_col - len(row) + 1)
                            if len(row) <= days_30_col:
                                row += [''] * (days_30_col - len(row) + 1)
                            if len(row) <= overall_col:
                                row += [''] * (overall_col - len(row) + 1)
                            data.append({
                                'range': f"{sheet_title}!{chr(65+li_ads_col)}{i}",
                                'values': [[li_ads]]
                            })
                            data.append({
                                'range': f"{sheet_title}!{chr(65+days_30_col)}{i}",
                                'values': [[days_30]]
                            })
                            data.append({
                                'range': f"{sheet_title}!{chr(65+overall_col)}{i}",
                                'values': [[overall]]
                            })
                if data:
                    body = {
                        'valueInputOption': 'RAW',
                        'data': data
                    }
                    service.spreadsheets().values().batchUpdate(
                        spreadsheetId=args.sheet_id,
                        body=body
                    ).execute()
                    logger.info(f"Updated only LI Ads?, 30 days, and Overall columns for {len(data)//3} rows.")
                else:
                    logger.warning("No matching profile URLs found for update.")
            except Exception as e:
                logger.error(f"Failed to update Google Sheet ad columns: {e}")
            
        except Exception as e:
            logger.error(f"Error during LinkedIn scraping: {str(e)}")
            return
        finally:
            browser.quit()
            
    except Exception as e:
        logger.error(f"Error in Chrome setup/execution: {str(e)}")
        return

    logger.info("\nScraping completed successfully")

def process_profiles(profiles_data: List[Dict], logger=None) -> Tuple[List[Dict], List[str]]:
    """Process profile data and extract company URLs for further scraping."""
    if logger is None:
        logger = logging.getLogger(__name__)
    
    processed_profiles = []
    company_urls = set()
    
    for profile in profiles_data:
        # Extract company details
        company_info = extract_company_details(profile)
        
        # Add company details to profile
        profile.update({
            "extractedCompanyDetails": company_info
        })
        
        # Add company URL to list for scraping if available
        if company_info["companyLinkedin"]:
            company_urls.add(company_info["companyLinkedin"])
        elif company_info["companyUrl"]:
            company_urls.add(company_info["companyUrl"])
        
        processed_profiles.append(profile)
    
    logger.info(f"Processed {len(processed_profiles)} profiles")
    logger.info(f"Found {len(company_urls)} unique company URLs to scrape")
    
    return processed_profiles, list(company_urls)

# --- Ad Count Scraper Functions ---

def capture_screenshot(browser, filename):
    """Capture a screenshot of the current page"""
    try:
        screenshots_dir = create_directory("screenshots")
        filepath = os.path.join(screenshots_dir, filename)
        browser.save_screenshot(filepath)
        logging.info(f"Screenshot saved to {filepath}")
        return filepath
    except Exception as e:
        logging.error(f"Error capturing screenshot: {e}")
        return None

def login_to_linkedin(browser, username=None, password=None, logger=None, visible_mode=False):
    """Login to LinkedIn with the provided credentials"""
    if logger is None:
        logger = logging.getLogger(__name__)
    
    # Use default credentials if none provided
    if username is None or password is None:
        username = DEFAULT_LINKEDIN_USERNAME
        password = DEFAULT_LINKEDIN_PASSWORD
        logger.info("Using default LinkedIn credentials")
    
    if not username or not password:
        logger.error("No LinkedIn credentials provided and no valid defaults found")
        return False
    
    try:
        logger.info("Logging in to LinkedIn...")
        browser.get("https://www.linkedin.com/login")
        logger.info("Login page loaded successfully")

        # Enter username
        username_elem = browser.find_element(By.ID, "username")
        username_elem.send_keys(username)
        logger.info("Username entered")

        # Enter password
        password_elem = browser.find_element(By.ID, "password")
        password_elem.send_keys(password)
        logger.info("Password entered")

        # Click login button
        login_button = browser.find_element(By.CSS_SELECTOR, "button[type='submit']")
        login_button.click()
        logger.info("Login form submitted")
        
        # Short delay before proceeding
        time.sleep(2)
        return True

    except Exception as e:
        logger.error(f"Error during login: {str(e)}")
        capture_screenshot(browser, "login_error.png")
        return False

def get_ads_count(browser, url, logger):
    """Extract the number of ads from a LinkedIn Ad Library page"""
    logger.info(f"Navigating to {url}")
    
    try:
        browser.get(url)
        time.sleep(8)  # Increased wait time for page load
        
        # Scroll to load all content
        browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        browser.execute_script("window.scrollTo(0, 0);")
        time.sleep(2)
        
        # Take screenshot for debugging
        screenshot_file = f"ad_library_{int(time.time())}.png"
        screenshot_path = capture_screenshot(browser, screenshot_file)
        logger.info(f"Screenshot saved to {screenshot_path}")
        
        # First try: Look for the ads count in the main container
        try:
            wait = WebDriverWait(browser, 10)
            count_element = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".search-results__total")))
            text = count_element.text.strip()
            if text:
                match = re.search(r'([\d,]+)\s+ads?\s+match', text)
                if match:
                    count = int(match.group(1).replace(',', ''))
                    logger.info(f"Found {count} ads from main container")
                    return count
        except Exception as e:
            logger.warning(f"Could not find count in main container: {e}")
        
        # Second try: Check various XPath patterns
        xpath_patterns = [
            "//div[contains(@class, 'search-results-container')]//span[contains(text(), 'ads match')]",
            "//div[contains(@class, 'search-results')]//h2[contains(text(), 'ads match')]",
            "//span[contains(@class, 'results-context-header')][contains(text(), 'ads match')]",
            "//span[contains(text(), 'ads match') or contains(text(), 'ad matches')]",
            "//div[contains(text(), 'ads match')]",
            "//h1[contains(text(), 'ads match')]",
            "//h2[contains(text(), 'ads match')]",
            "//div[contains(@class, 'search-results')]//span[contains(text(), 'ads')]",
            "//div[contains(@class, 'search-results__header')]//span[contains(text(), 'ads')]"
        ]
        
        for xpath in xpath_patterns:
            logger.info(f"Trying XPath: {xpath}")
            try:
                elements = browser.find_elements(By.XPATH, xpath)
                if elements:
                    logger.info(f"Found {len(elements)} potential elements")
                    
                    for elem in elements:
                        text = elem.text.strip()
                        logger.info(f"Element text: '{text}'")
                        
                        if text:
                            match = re.search(r'([\d,]+)\s+ads?\s+match', text)
                            if match:
                                count_str = match.group(1).replace(',', '')
                                count = int(count_str)
                                logger.info(f"Found {count} ads from element text")
                                return count
            except Exception as e:
                logger.warning(f"Error processing XPath {xpath}: {e}")
                continue
        
        # Third try: Use JavaScript to get computed text
        logger.info("Trying JavaScript to get computed text")
        try:
            js_script = """
                return Array.from(document.querySelectorAll('*')).find(el => 
                    el.textContent.match(/[\\d,]+ ads? match/)
                )?.textContent || '';
            """
            text = browser.execute_script(js_script)
            if text:
                logger.info(f"Found text via JavaScript: {text}")
                match = re.search(r'([\d,]+)\s+ads?\s+match', text)
                if match:
                    count_str = match.group(1).replace(',', '')
                    count = int(count_str)
                    logger.info(f"Found {count} ads from JavaScript")
                    return count
        except Exception as e:
            logger.warning(f"Error executing JavaScript: {e}")
        
        # Final try: Check full page source
        logger.info("Checking full page source as fallback")
        page_source = browser.page_source
        
        # Look for both "ads match" and "ad matches" patterns
        patterns = [
            r'([\d,]+)\s+ads?\s+match',
            r'([\d,]+)\s+ads?\s+found',
            r'showing\s+([\d,]+)\s+ads?',
            r'found\s+([\d,]+)\s+ads?'
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, page_source, re.IGNORECASE)
        if matches:
            counts = [int(m.replace(',', '')) for m in matches]
            count = max(counts)
            logger.info(f"Found {count} ads in page source (largest match)")
            return count
        
        if re.search(r'No ads to show|No results found|No ads match', page_source, re.IGNORECASE):
            logger.info("No ads found on this page")
            return 0
        
        logger.warning("Could not find ad count on the page")
        return None
            
    except Exception as e:
        logger.error(f"Error loading {url}: {e}")
        return None

def scrape_ad_counts(company_details, browser, logger, wait_time=2):
    """Scrape ad counts for each company"""
    for company in company_details:
        company_id = company.get('company_id')
        company_name = company.get('name', 'Unknown')
        
        if not company_id:
            logger.warning(f"Skipping {company_name}: No company ID available")
            continue
        
        logger.info(f"Processing {company_name} (ID: {company_id})...")
        
        # First get all-time ads count
        url = f"https://www.linkedin.com/ad-library/search?companyIds={company_id}"
        logger.info(f"Navigating to {url}")
        browser.get(url)
        
        # Take screenshot
        timestamp = int(time.time())
        screenshot_file = f"ad_library_{timestamp}.png"
        capture_screenshot(browser, screenshot_file)
        
        logger.info("Waiting for ad count element to load...")
        time.sleep(wait_time)  # Initial wait
        
        # Try different XPath expressions to find the ad count
        xpath_patterns = [
            "//div[contains(@class, 'search-results-container')]//span[contains(text(), 'ads match')]",
            "//div[contains(@class, 'search-results')]//h2[contains(text(), 'ads match')]",
            "//span[contains(@class, 'results-context-header')][contains(text(), 'ads match')]",
            "//span[contains(text(), 'ads match') or contains(text(), 'ad matches')]",
            "//div[contains(text(), 'ads match')]",
            "//h1[contains(text(), 'ads match')]",
            "//h2[contains(text(), 'ads match')]",
            "//div[contains(@class, 'search-results')]//span[contains(text(), 'ads')]"
        ]
        
        all_time_count = 0
        for xpath in xpath_patterns:
            logger.info(f"Trying XPath: {xpath}")
            try:
                elements = browser.find_elements(By.XPATH, xpath)
                if elements:
                    logger.info(f"Found {len(elements)} potential elements")
                    for element in elements:
                        text = element.text
                        logger.info(f"Element text: '{text}'")
                        if 'ads match' in text.lower() or 'ad matches' in text.lower():
                            count = int(''.join(filter(str.isdigit, text)))
                            logger.info(f"Found {count} ads from element text")
                            all_time_count = count
                            break
                    if all_time_count > 0:
                        break
            except Exception as e:
                logger.warning(f"Error with XPath '{xpath}': {str(e)}")
                continue
        
        # If no count found, try JavaScript
        if all_time_count == 0:
            logger.info("Trying JavaScript to get computed text")
            try:
                elements = browser.find_elements(By.CSS_SELECTOR, '.search-results')
                for element in elements:
                    text = browser.execute_script("return arguments[0].textContent", element)
                    if 'ads match' in text.lower():
                        count = int(''.join(filter(str.isdigit, text)))
                        all_time_count = count
                        break
            except Exception as e:
                logger.warning(f"Error with JavaScript approach: {str(e)}")
        
        # If still no count, check page source as last resort
        if all_time_count == 0:
            logger.info("Checking full page source as fallback")
            page_source = browser.page_source
            if 'ads match' in page_source.lower():
                matches = re.findall(r'(\d+)\s+ads?\s+match', page_source.lower())
                if matches:
                    all_time_count = int(matches[0])
        
        # Now get last 30 days count
        url = f"https://www.linkedin.com/ad-library/search?companyIds={company_id}&dateOption=last-30-days"
        logger.info(f"Navigating to {url}")
        browser.get(url)
        
        # Take screenshot
        timestamp = int(time.time())
        screenshot_file = f"ad_library_{timestamp}.png"
        capture_screenshot(browser, screenshot_file)
        
        logger.info("Waiting for ad count element to load...")
        time.sleep(wait_time)
        
        last_30_days_count = 0
        for xpath in xpath_patterns:
            logger.info(f"Trying XPath: {xpath}")
            try:
                elements = browser.find_elements(By.XPATH, xpath)
                if elements:
                    logger.info(f"Found {len(elements)} potential elements")
                    for element in elements:
                        text = element.text
                        logger.info(f"Element text: '{text}'")
                        if 'ads match' in text.lower() or 'ad matches' in text.lower():
                            count = int(''.join(filter(str.isdigit, text)))
                            logger.info(f"Found {count} ads from element text")
                            last_30_days_count = count
                            break
                    if last_30_days_count > 0:
                        break
            except Exception as e:
                logger.warning(f"Error with XPath '{xpath}': {str(e)}")
                continue
                
        # If no count found, try JavaScript
        if last_30_days_count == 0:
            logger.info("Trying JavaScript to get computed text")
            try:
                elements = browser.find_elements(By.CSS_SELECTOR, '.search-results')
                for element in elements:
                    text = browser.execute_script("return arguments[0].textContent", element)
                    if 'ads match' in text.lower():
                        count = int(''.join(filter(str.isdigit, text)))
                        last_30_days_count = count
                        break
            except Exception as e:
                logger.warning(f"Error with JavaScript approach: {str(e)}")
        
        # If still no count, check page source as last resort
        if last_30_days_count == 0:
            logger.info("Checking full page source as fallback")
            page_source = browser.page_source
            if 'ads match' in page_source.lower():
                matches = re.findall(r'(\d+)\s+ads?\s+match', page_source.lower())
                if matches:
                    last_30_days_count = int(matches[0])
            else:
                logger.info("No ads found on this page")
        
        # Store the results
        logger.info(f"  All time: {all_time_count}, Last 30 days: {last_30_days_count}")
        print(f"{company_name} - All time: {all_time_count}, Last 30 days: {last_30_days_count}")
        
        company['all_time_ads'] = all_time_count
        company['last_30_days_ads'] = last_30_days_count
        
        # Add a small delay between companies
        time.sleep(wait_time)
    
    return company_details

def update_sheet_with_ad_data(service, sheet_id: str, profile_data: List[Dict], ad_data: Dict[str, Dict], logger=None):
    """Update Google Sheet with ad count data"""
    if logger is None:
        logger = logging.getLogger(__name__)
        
    try:
        # Get the sheet metadata
        sheet = service.spreadsheets().get(spreadsheetId=sheet_id).execute()
        sheet_title = sheet['sheets'][0]['properties']['title']
        
        # Get existing data
        result = service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range=f"{sheet_title}!A:Z"
        ).execute()
        
        existing_data = result.get('values', [])
        if not existing_data:
            logger.error("No data found in sheet")
            return False
            
        # Find the columns for ad counts
        header_row = existing_data[0]
        try:
            all_time_col = header_row.index("All Time Ads") + 1  # Convert to 1-based index
            last_30_col = header_row.index("Last 30 Days Ads") + 1
            company_id_col = header_row.index("Company ID") + 1
        except ValueError:
            # Add new columns if they don't exist
            all_time_col = len(header_row) + 1
            last_30_col = all_time_col + 1
            
            # Update header row
            update_range = f"{sheet_title}!{chr(64+all_time_col)}1:{chr(64+last_30_col)}1"
            service.spreadsheets().values().update(
                spreadsheetId=sheet_id,
                range=update_range,
                valueInputOption="RAW",
                body={
                    "values": [["All Time Ads", "Last 30 Days Ads"]]
                }
            ).execute()
            logger.info("Added new columns for ad counts")
        
        # Prepare batch updates
        batch_updates = []
        for row_idx, row in enumerate(existing_data[1:], start=2):  # Skip header, use 1-based index
            try:
                company_id = str(row[company_id_col - 1]) if len(row) >= company_id_col else None
                if company_id and company_id in ad_data:
                    # Update ad count cells
                    all_time_value = ad_data[company_id]['all_time']
                    last_30_value = ad_data[company_id]['last_30_days']
                    
                    batch_updates.extend([
                        {
                            'range': f"{sheet_title}!{chr(64+all_time_col)}{row_idx}",
                            'values': [[all_time_value]]
                        },
                        {
                            'range': f"{sheet_title}!{chr(64+last_30_col)}{row_idx}",
                            'values': [[last_30_value]]
                        }
                    ])
            except Exception as e:
                logger.warning(f"Error processing row {row_idx}: {str(e)}")
                continue
        
        # Execute batch update
        if batch_updates:
            body = {
                'valueInputOption': 'RAW',
                'data': batch_updates
            }
            service.spreadsheets().values().batchUpdate(
                spreadsheetId=sheet_id,
                body=body
            ).execute()
            logger.info(f"Updated {len(batch_updates)//2} rows with ad data")
            return True
        else:
            logger.warning("No updates to perform")
            return False
            
    except Exception as e:
        logger.error(f"Error updating sheet: {str(e)}")
        return False

def trigger_collection(inputs: List[Dict[str, str]], dataset_id: str = None) -> Optional[str]:
    """Trigger a new data collection and return the snapshot ID."""
    if dataset_id is None:
        dataset_id = COMPANY_DATASET_ID  # Default to company scraper
        
    url = f"{BRIGHT_DATA_BASE_URL}/trigger"
    params = {
        "dataset_id": dataset_id,
        "include_errors": "true"
    }
    headers = {
        "Authorization": f"Bearer {BRIGHT_DATA_API_TOKEN}",
        "Content-Type": "application/json"
    }
    
    logging.info(f"Triggering new data collection for dataset {dataset_id}...")
    response = requests.post(url, headers=headers, params=params, json=inputs)
    
    if response.status_code != 200:
        logging.error(f"Error: Collection trigger failed with status code {response.status_code}")
        logging.error(f"Response: {response.text}")
        return None
        
    try:
        result = response.json()
        snapshot_id = result.get('snapshot_id')
        if not snapshot_id:
            logging.error("Error: No snapshot_id in response")
            return None
        logging.info(f"Collection triggered successfully. Snapshot ID: {snapshot_id}")
        return snapshot_id
    except Exception as e:
        logging.error(f"Error parsing response: {e}")
        return None

def check_progress(snapshot_id: str) -> str:
    """Check the progress of a collection."""
    url = f"{BRIGHT_DATA_BASE_URL}/progress/{snapshot_id}"
    headers = {"Authorization": f"Bearer {BRIGHT_DATA_API_TOKEN}"}
    
    try:
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            logging.error(f"Error checking progress: {response.status_code}")
            logging.error(f"Response: {response.text}")
            return 'failed'
            
        result = response.json()
        return result.get('status', 'failed')
    except Exception as e:
        logging.error(f"Error checking progress: {e}")
        return 'failed'

def wait_for_completion(snapshot_id: str, timeout: int = 300, check_interval: int = 5) -> bool:
    """Wait for the collection to complete."""
    start_time = time.time()
    logging.info("\nWaiting for data collection to complete...")
    
    while time.time() - start_time < timeout:
        status = check_progress(snapshot_id)
        
        if status == 'ready':
            logging.info("Data collection completed successfully!")
            return True
        elif status == 'failed':
            logging.error("Data collection failed!")
            return False
        elif status == 'running':
            logging.info(f"Status: {status}...")
            time.sleep(check_interval)
        else:
            logging.error(f"Unknown status: {status}")
            return False
    
    logging.error("Timeout reached while waiting for completion")
    return False

def fetch_results(snapshot_id: str, format: str = 'json') -> Optional[Union[List[Dict], Dict]]:
    """Fetch the results of a completed collection."""
    url = f"{BRIGHT_DATA_BASE_URL}/snapshot/{snapshot_id}"
    headers = {"Authorization": f"Bearer {BRIGHT_DATA_API_TOKEN}"}
    params = {"format": format}
    
    try:
        logging.info("Fetching results...")
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code != 200:
            logging.error(f"Error fetching results: {response.status_code}")
            logging.error(f"Response: {response.text}")
            return None
            
        return response.json()
    except Exception as e:
        logging.error(f"Error fetching results: {e}")
        return None

def main():
    parser = argparse.ArgumentParser(description="LinkedIn Profile and Ad Count Scraper")
    parser.add_argument('--sheet-id', required=True, help='Google Sheet ID containing profile URLs')
    parser.add_argument('--output', default='profile_combined_data.csv', help='Output CSV file')
    parser.add_argument('--intermediate', default='profile_data.json', help='Intermediate JSON file for profile data')
    parser.add_argument('--company-data', default='company_data.json', help='Company data JSON file')
    parser.add_argument('--visible', action='store_true', help='Show the browser window during execution')
    parser.add_argument('--wait', type=int, default=2, help='Additional wait time after page load in seconds')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--apify-token', required=True, help='Apify API token')
    parser.add_argument('--linkedin-username', help='LinkedIn username')
    parser.add_argument('--linkedin-password', help='LinkedIn password')
    args = parser.parse_args()

    # Setup logging
    logger = setup_logging()
    logger.setLevel(logging.DEBUG if args.debug else logging.INFO)
    logger.info("Starting LinkedIn Profile and Ad Count Scraper")

    # Set Apify token
    global APIFY_API_TOKEN
    APIFY_API_TOKEN = args.apify_token

    # Get Google Sheets service
    service = get_google_sheets_service()
    if not service:
        logger.error("Failed to initialize Google Sheets service")
        return
    
    # STEP 1: Scrape LinkedIn Profiles using Apify
    logger.info("\n=== STEP 1: Scraping LinkedIn Profiles using Apify ===")
    
    # Read profiles from Google Sheet
    profile_urls = read_profiles_from_sheet(args.sheet_id, service, logger)
    if not profile_urls:
        logger.error("No valid profiles found to process. Exiting.")
        return

    logger.info(f"Found {len(profile_urls)} profiles to scrape")
    profile_data = scrape_linkedin_profiles(profile_urls, APIFY_API_TOKEN, logger)
    if not profile_data:
        logger.error("Failed to scrape profiles with Apify")
        return

    # Save intermediate profile results
    save_results(profile_data, args.output, args.intermediate, logger)
    logger.info("Profile data saved successfully")

    # STEP 2: Get Company Data using Bright Data
    logger.info("\n=== STEP 2: Getting Company Data using Bright Data ===")
    
    # Extract company URLs
    company_urls = []
    for profile in profile_data:
        experiences = profile.get('experiences', [])
        if experiences and len(experiences) > 0:
            company_url = experiences[0].get('companyLink1', 'N/A')
            if company_url != 'N/A' and 'linkedin.com/company/' in company_url:
                formatted_url = format_company_url(company_url)
                company_urls.append({"url": formatted_url})
                logger.info(f"Added company URL: {formatted_url}")

    if not company_urls:
        logger.error("No valid company URLs found")
        return

    # Trigger Bright Data collection
    logger.info(f"Making batch request for {len(company_urls)} companies using Bright Data...")
    snapshot_id = trigger_collection(company_urls)
    if not snapshot_id:
        logger.error("Failed to create snapshot for company data")
        return

    # Wait for snapshot with proper retry logic
    max_retries = 10
    retry_delay = 30  # seconds
    company_data = None
    
    for attempt in range(max_retries):
        if wait_for_completion(snapshot_id):
            logger.info("Attempting to fetch company data...")
            try:
                company_data = fetch_results(snapshot_id)
                if company_data:
                    logger.info("Successfully fetched company data")
                    # Save company data
                    with open(args.company_data, 'w') as f:
                        json.dump(company_data, f, indent=2)
                    logger.info(f"Company data saved to {args.company_data}")
                    break
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1}: Failed to fetch results: {str(e)}")
        
        if attempt < max_retries - 1:
            logger.info(f"Waiting {retry_delay} seconds before retrying...")
            time.sleep(retry_delay)
    
    if not company_data:
        logger.error("Failed to fetch company data after all retries")
        return

    # STEP 3: Scrape Ad Counts using Chrome
    logger.info("\n=== STEP 3: Scraping Ad Counts using Chrome ===")
    
    # Setup Chrome
    chrome_options = Options()
    if not args.visible:
        chrome_options.add_argument('--headless=new')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--disable-software-rasterizer')
    
    try:
        # Initialize ChromeDriver with correct path handling
        driver_manager = ChromeDriverManager()
        driver_path = driver_manager.install()
        logger.info(f"Initial ChromeDriver path: {driver_path}")
        
        # Get the actual chromedriver executable path
        if 'chromedriver-mac-arm64' in driver_path:
            driver_path = os.path.join(os.path.dirname(driver_path), 'chromedriver')
        elif 'THIRD_PARTY_NOTICES' in driver_path:
            driver_path = os.path.join(os.path.dirname(driver_path), 'chromedriver')
            
        if not os.path.exists(driver_path):
            # Try to find chromedriver in the parent directory
            parent_dir = os.path.dirname(os.path.dirname(driver_path))
            potential_path = os.path.join(parent_dir, 'chromedriver')
            if os.path.exists(potential_path):
                driver_path = potential_path
            else:
                raise Exception(f"ChromeDriver executable not found in {driver_path} or {potential_path}")
        
        logger.info(f"Using ChromeDriver executable at: {driver_path}")
        os.chmod(driver_path, 0o755)  # Ensure executable permissions
        
        # Create browser instance
        service = Service(executable_path=driver_path)
        browser = webdriver.Chrome(service=service, options=chrome_options)
        browser.set_window_size(1920, 1080)
        
        try:
            # Login to LinkedIn
            if not login_to_linkedin(browser, args.linkedin_username, args.linkedin_password, logger=logger):
                logger.error("Failed to login to LinkedIn")
                return

            # Process each company
            for company in company_data:
                company_id = company.get('company_id')
                company_name = company.get('name', 'Unknown')
                
                if not company_id:
                    logger.warning(f"No company ID for {company_name}, skipping...")
                    continue
                
                logger.info(f"\nProcessing {company_name} (ID: {company_id})")
                
                # Get all-time ads count
                url = f"https://www.linkedin.com/ad-library/search?companyIds={company_id}"
                all_time_count = get_ads_count(browser, url, logger)
                    
                # Get last 30 days count
                url = f"https://www.linkedin.com/ad-library/search?companyIds={company_id}&dateOption=last-30-days"
                last_30_count = get_ads_count(browser, url, logger)
                
                # Update company data
                company['all_time_ads'] = all_time_count if all_time_count is not None else 0
                company['last_30_days_ads'] = last_30_count if last_30_count is not None else 0
                
                logger.info(f"Ad counts for {company_name}:")
                logger.info(f"  All time: {company['all_time_ads']}")
                logger.info(f"  Last 30 days: {company['last_30_days_ads']}")
                
                # Small delay between companies
                time.sleep(args.wait)
                
            # Save updated company data
            with open(args.company_data, 'w') as f:
                json.dump(company_data, f, indent=2)
            logger.info(f"\nUpdated company data saved to {args.company_data}")

            logger.info("Company data before final merge (URLs, names, and IDs):")
            if company_data:
                for i, comp_entry in enumerate(company_data):
                    c_url = comp_entry.get('url', 'N/A_URL_IN_COMP_DATA')
                    c_name = comp_entry.get('name', 'N/A_NAME_IN_COMP_DATA')
                    c_id = comp_entry.get('company_id', 'N/A_ID_IN_COMP_DATA')
                    # Also log the ad counts as they exist in company_data at this point
                    c_all_ads = comp_entry.get('all_time_ads', 'N/A_ALL_ADS')
                    c_30d_ads = comp_entry.get('last_30_days_ads', 'N/A_30D_ADS')
                    logger.info(f"  Entry {i}: Name='{c_name}', ID='{c_id}', URL='{c_url}', FormattedURL='{format_company_url(c_url)}', AllAds='{c_all_ads}', 30dAds='{c_30d_ads}'")
            else:
                logger.warning("  company_data list is empty or None before final merge.")
            
            # Create final combined data
            combined_data = []
            def extract_company_id_from_url(url):
                match = re.search(r"/company/(\d+)", str(url))
                return match.group(1) if match else None
            for profile in profile_data:
                profile_company_id = None
                raw_company_link = "N/A_RAW_LINK" # For logging
                if profile.get('experiences') and len(profile['experiences']) > 0:
                    raw_company_link = profile['experiences'][0].get('companyLink1', 'N/A_IN_EXPERIENCE')
                    profile_company_id = extract_company_id_from_url(raw_company_link)
                logger.info(f"Attempting to merge for profile: '{profile.get('fullName', 'N/A_PROFILE_NAME')}'")
                logger.info(f"  Raw companyLink1 from profile: '{raw_company_link}'")
                logger.info(f"  Extracted profile_company_id for matching: '{profile_company_id}'")
                company_info_for_profile = {} # Default to empty if no match
                if profile_company_id and profile_company_id != 'N/A': # Only attempt match if profile_company_id is valid
                    if not company_data:
                        logger.warning("  Company data list is empty or None during merge step. Cannot find a match.")
                    else:
                        match_found_in_company_data = False
                        for c_idx, company_entry in enumerate(company_data):
                            company_data_id = str(company_entry.get('company_id', ''))
                            logger.info(f"    Checking against company_data entry #{c_idx}: Name='{company_entry.get('name', 'N/A_COMPANY_NAME')}', ID='{company_data_id}'")
                            if company_data_id == str(profile_company_id):
                                company_info_for_profile = company_entry
                                logger.info(f"  SUCCESSFUL MATCH: Profile '{profile.get('fullName', 'N/A_PROFILE_NAME')}' (ID: '{profile_company_id}')")
                                logger.info(f"    Matched with company_data entry: Name='{company_entry.get('name', 'N/A_COMPANY_NAME')}', ID='{company_data_id}'")
                                match_found_in_company_data = True
                                break # Found a match, no need to check further for this profile
                        if not match_found_in_company_data:
                            logger.warning(f"  NO MATCH FOUND for profile '{profile.get('fullName', 'N/A_PROFILE_NAME')}' (ID: '{profile_company_id}') in {len(company_data)} company_data entries.")
                else:
                    logger.warning(f"  SKIPPING MATCH for profile '{profile.get('fullName', 'N/A_PROFILE_NAME')}' because its company ID is invalid or missing (Raw: '{raw_company_link}', Extracted: '{profile_company_id}').")
                company_info = company_info_for_profile # Use the matched company_info or the default empty {}
                combined_record = {
                    'profile_name': profile.get('fullName', 'N/A'),
                    'profile_url': profile.get('linkedinUrl', 'N/A'),
                    'profile_location': profile.get('addressWithCountry', 'N/A'),
                    'profile_headline': profile.get('headline', 'N/A'),
                    'profile_connections': profile.get('connections', 'N/A'),
                    'profile_followers': profile.get('followers', 'N/A'),
                    'company_name': company_info.get('name', 'N/A'),
                    'company_url': company_info.get('url', 'N/A'),
                    'company_id': company_info.get('company_id', 'N/A'),
                    'company_industry': company_info.get('industries', 'N/A'),
                    'company_size': company_info.get('company_size', 'N/A'),
                    'company_followers': company_info.get('followers', 'N/A'),
                    'all_time_ads': company_info.get('all_time_ads', 0),
                    'last_30_days_ads': company_info.get('last_30_days_ads', 0),
                    'linkedin_ads': 'y' if (company_info.get('all_time_ads', 0) > 0 or company_info.get('last_30_days_ads', 0) > 0) else 'n',
                    'snapshot_date': datetime.datetime.now().strftime("%Y-%m-%d")
                }
                combined_data.append(combined_record)
            # Save to CSV
            df = pd.DataFrame(combined_data)
            df.to_csv(args.output, index=False)
            logger.info(f"\nFinal combined results saved to {args.output}")

            # --- NEW: Update ONLY the ad columns in Google Sheet ---
            try:
                service = get_google_sheets_service()
                sheet = service.spreadsheets().get(spreadsheetId=args.sheet_id).execute()
                sheet_title = sheet['sheets'][0]['properties']['title']
                # Get existing data
                result = service.spreadsheets().values().get(
                    spreadsheetId=args.sheet_id,
                    range=f"{sheet_title}!A1:Z"
                ).execute()
                values = result.get('values', [])
                if not values or len(values) < 2:
                    logger.error('No data found in the sheet')
                    return
                headers = values[0]
                # Find column indices
                try:
                    profile_url_col = headers.index('profileUrl')
                    li_ads_col = headers.index('LI Ads?')
                    days_30_col = headers.index('30 days')
                    overall_col = headers.index('Overall')
                except ValueError as e:
                    logger.error(f"Required column missing: {e}")
                    return
                # Build a lookup from combined_data by profile_url
                combined_lookup = {row.get('profile_url', ''): row for row in combined_data}
                # Prepare batch update
                data = []
                for i, row in enumerate(values[1:], start=2):  # 1-based index, skip header
                    if len(row) > profile_url_col:
                        url = row[profile_url_col]
                        if url in combined_lookup:
                            cdata = combined_lookup[url]
                            li_ads = 'y' if (cdata.get('all_time_ads', 0) > 0 or cdata.get('last_30_days_ads', 0) > 0) else 'n'
                            days_30 = cdata.get('last_30_days_ads', 0)
                            overall = cdata.get('all_time_ads', 0)
                            # Prepare update for each column
                            if len(row) <= li_ads_col:
                                row += [''] * (li_ads_col - len(row) + 1)
                            if len(row) <= days_30_col:
                                row += [''] * (days_30_col - len(row) + 1)
                            if len(row) <= overall_col:
                                row += [''] * (overall_col - len(row) + 1)
                            data.append({
                                'range': f"{sheet_title}!{chr(65+li_ads_col)}{i}",
                                'values': [[li_ads]]
                            })
                            data.append({
                                'range': f"{sheet_title}!{chr(65+days_30_col)}{i}",
                                'values': [[days_30]]
                            })
                            data.append({
                                'range': f"{sheet_title}!{chr(65+overall_col)}{i}",
                                'values': [[overall]]
                            })
                if data:
                    body = {
                        'valueInputOption': 'RAW',
                        'data': data
                    }
                    service.spreadsheets().values().batchUpdate(
                        spreadsheetId=args.sheet_id,
                        body=body
                    ).execute()
                    logger.info(f"Updated only LI Ads?, 30 days, and Overall columns for {len(data)//3} rows.")
                else:
                    logger.warning("No matching profile URLs found for update.")
            except Exception as e:
                logger.error(f"Failed to update Google Sheet ad columns: {e}")
            
        except Exception as e:
            logger.error(f"Error during LinkedIn scraping: {str(e)}")
            return
        finally:
            browser.quit()
            
    except Exception as e:
        logger.error(f"Error in Chrome setup/execution: {str(e)}")
        return

    logger.info("\nScraping completed successfully")

if __name__ == "__main__":
    main() 
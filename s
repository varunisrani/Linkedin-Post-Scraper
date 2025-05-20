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
from typing import List, Dict, Union, Optional
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# Bright Data API settings
BRIGHT_DATA_API_TOKEN = "09aeea2d-50fc-42ec-9f2a-e3b8cfcc59be"
PROFILE_DATASET_ID = "gd_l1viktl72bvl7bjuj0"  # Profile scraper dataset ID
COMPANY_DATASET_ID = "gd_l1vikfnt1wgvvqz95w"  # Company scraper dataset ID - Updated to match combined scraper
BASE_URL = "https://api.brightdata.com/datasets/v3"

# LinkedIn credentials (default values)
DEFAULT_LINKEDIN_USERNAME = "kvtvpxgaming@gmail.com"
DEFAULT_LINKEDIN_PASSWORD = "Hello@1055"

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
                profiles.append({"url": row['profileUrl'].strip()})
    return profiles

def trigger_collection(inputs: List[Dict[str, str]], dataset_id: str = None) -> Optional[str]:
    """Trigger a new data collection and return the snapshot ID."""
    if dataset_id is None:
        dataset_id = PROFILE_DATASET_ID  # Default to profile scraper
        
    url = f"{BASE_URL}/trigger"
    params = {
        "dataset_id": dataset_id,
        "include_errors": "true"
    }
    headers = {
        "Authorization": f"Bearer {BRIGHT_DATA_API_TOKEN}",
        "Content-Type": "application/json"
    }
    
    print(f"Triggering new data collection for dataset {dataset_id}...")
    response = requests.post(url, headers=headers, params=params, json=inputs)
    
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
    """Check the progress of a collection."""
    url = f"{BASE_URL}/progress/{snapshot_id}"
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

def wait_for_completion(snapshot_id: str, timeout: int = 300, check_interval: int = 5) -> bool:
    """Wait for the collection to complete."""
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
    """Fetch the results of a completed collection."""
    url = f"{BASE_URL}/snapshot/{snapshot_id}"
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

def format_company_url(url: str) -> str:
    """Format company URL to be compatible with the API requirements."""
    if not url or url == 'N/A':
        return url
        
    # Remove tracking parameters and clean up the URL
    url = url.split('?')[0]
    
    # Extract company name from URL
    company_name = url.split('/company/')[-1] if '/company/' in url else ''
    if company_name:
        # Format URL to match the working pattern
        return f"https://www.linkedin.com/company/{company_name}"
    
    return url

def extract_company_details(profile_data: Union[List[Dict], Dict]) -> List[Dict]:
    """Extract key company details from the profile API response and fetch company IDs."""
    # Handle both list and single profile responses
    profiles = profile_data if isinstance(profile_data, list) else [profile_data]
    company_details = []
    company_urls_map = {}  # Map to track which profile each company URL belongs to
    
    # First pass: Collect all company URLs and initialize company details
    company_requests = []
    for profile in profiles:
        if isinstance(profile, str):
            # Skip string entries
            continue
            
        # Get current company info
        current_company = profile.get('current_company', {})
        company_url = current_company.get('link', 'N/A')
        
        # Initialize company detail with profile info
        company_detail = {
            'profile_name': profile.get('name', 'N/A'),
            'profile_url': profile.get('url', 'N/A'),
            'company_name': current_company.get('name', 'N/A'),
            'company_url': company_url,
            'company_id': 'N/A',  # Will be updated after company scraping
            'location': profile.get('location', 'N/A'),
            'followers': profile.get('followers', 'N/A'),
            'snapshot_date': datetime.datetime.now().strftime("%Y-%m-%d")
        }
        
        company_details.append(company_detail)
        
        # If we have a valid company URL, add it to our batch request
        if company_url != 'N/A':
            formatted_url = format_company_url(company_url)
            if formatted_url != company_url:
                logging.info(f"Reformatted company URL from {company_url} to {formatted_url}")
            
            company_requests.append({"url": formatted_url})
            company_urls_map[formatted_url] = len(company_details) - 1  # Store index of company detail
    
    # If we have company URLs to process, make a single batch API call
    if company_requests:
        try:
            logging.info(f"Making batch request for {len(company_requests)} companies")
            
            # Trigger collection for all companies at once
            snapshot_id = trigger_collection(company_requests, COMPANY_DATASET_ID)
            if snapshot_id:
                if wait_for_completion(snapshot_id):
                    company_results = fetch_results(snapshot_id)
                    if company_results:
                        # Process results and update company details
                        results_list = company_results if isinstance(company_results, list) else [company_results]
                        
                        for company_data in results_list:
                            company_url = company_data.get('url', '')
                            if company_url in company_urls_map:
                                # Find the corresponding company detail to update
                                idx = company_urls_map[company_url]
                                
                                # Update company details with actual data
                                company_details[idx].update({
                                    'company_id': company_data.get('company_id', 'N/A'),
                                    'industry': company_data.get('industries', 'N/A'),
                                    'company_size': company_data.get('company_size', 'N/A'),
                                    'organization_type': company_data.get('organization_type', 'N/A'),
                                    'website': company_data.get('website', 'N/A'),
                                    'about': company_data.get('about', 'N/A')
                                })
                                
                                logging.info(f"Successfully updated data for {company_details[idx]['company_name']}")
                            else:
                                logging.warning(f"Could not match company data for URL: {company_url}")
                    else:
                        logging.warning("No company data found in batch response")
                else:
                    logging.warning("Batch company data collection failed or timed out")
            else:
                logging.warning("Failed to trigger batch company data collection")
                
        except Exception as e:
            logging.error(f"Error in batch company data fetch: {e}")
    
    return company_details

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

def login_to_linkedin(browser, username=None, password=None, logger=None):
    """Login to LinkedIn with the provided credentials"""
    if logger is None:
        logger = logging.getLogger("linkedin_profile_scraper")
        
    if username is None or password is None:
        username = DEFAULT_LINKEDIN_USERNAME
        password = DEFAULT_LINKEDIN_PASSWORD
    
    logger.info("Logging in to LinkedIn...")
    browser.get('https://www.linkedin.com/login')
    
    try:
        WebDriverWait(browser, 10).until(EC.presence_of_element_located((By.ID, "username")))
        logger.info("Login page loaded successfully")
        
        elementID = browser.find_element(By.ID, "username")
        elementID.send_keys(username)
        logger.info("Username entered")
        
        elementID = browser.find_element(By.ID, "password")
        elementID.send_keys(password)
        logger.info("Password entered")
        
        elementID.submit()
        logger.info("Login form submitted")
        
        WebDriverWait(browser, 30).until(EC.url_changes('https://www.linkedin.com/login'))
        logger.info("Successfully logged in to LinkedIn")
        
        if "/feed/" in browser.current_url:
            logger.info("Confirmed login success - redirected to LinkedIn feed")
        else:
            logger.warning(f"Redirected to: {browser.current_url}")
            if "checkpoint" in browser.current_url:
                logger.warning("Security checkpoint detected. Waiting for manual verification...")
                capture_screenshot(browser, "linkedin_checkpoint.png")
                logger.warning("MANUAL ACTION REQUIRED: Please check the browser window and complete any security verification")
                
                if not browser.headless:
                    logger.info("Waiting for manual verification (30 seconds)...")
                    time.sleep(30)
                    logger.info("Resuming after checkpoint wait")
    
    except Exception as e:
        logger.error(f"Error during login process: {e}")
        logger.warning("Login process may have failed or took longer than expected, but continuing...")
    
    screenshot_path = capture_screenshot(browser, "linkedin_login_status.png")
    
    if screenshot_path:
        logger.info(f"Login status screenshot saved to: {screenshot_path}")
    
    return "/feed/" in browser.current_url

def get_ads_count(browser, url, logger):
    """Extract the number of ads from a LinkedIn Ad Library page"""
    logger.info(f"Navigating to {url}")
    
    try:
        browser.get(url)
        time.sleep(8)
        
        browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        browser.execute_script("window.scrollTo(0, 0);")
        time.sleep(2)
        
        screenshot_file = f"ad_library_{int(time.time())}.png"
        screenshot_path = capture_screenshot(browser, screenshot_file)
        logger.info(f"Screenshot saved to {screenshot_path}")
        
        logger.info("Waiting for ad count element to load...")
        
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
        
        logger.info("Checking full page source as fallback")
        page_source = browser.page_source
        
        matches = re.findall(r'([\d,]+)\s+ads?\s+match', page_source)
        if matches:
            counts = [int(m.replace(',', '')) for m in matches]
            count = max(counts)
            logger.info(f"Found {count} ads in page source (largest match)")
            return count
        
        if re.search(r'No ads to show|No results found', page_source, re.IGNORECASE):
            logger.info("No ads found on this page")
            return 0
        
        logger.warning("Could not find ad count on the page")
        return None
            
    except Exception as e:
        logger.error(f"Error loading {url}: {e}")
        return None

def scrape_ad_counts(company_details, browser, logger, wait_time=2):
    """Scrape ad counts for companies"""
    for company in company_details:
        company_name = company['company_name']
        company_id = company['company_id']
        
        if company_id == 'N/A' or not company_id:
            logger.warning(f"Skipping {company_name}: No company ID available")
            company['all_time_ads'] = None
            company['last_30_days_ads'] = None
            continue
        
        logger.info(f"Processing {company_name} (ID: {company_id})...")
        
        all_time_url = f"https://www.linkedin.com/ad-library/search?companyIds={company_id}"
        last_30_url = f"https://www.linkedin.com/ad-library/search?companyIds={company_id}&dateOption=last-30-days"
        
        all_time_count = get_ads_count(browser, all_time_url, logger)
        last_30_count = get_ads_count(browser, last_30_url, logger)
        
        logger.info(f"  All time: {all_time_count}, Last 30 days: {last_30_count}")
        print(f"{company_name} - All time: {all_time_count}, Last 30 days: {last_30_count}")
        
        company['all_time_ads'] = all_time_count
        company['last_30_days_ads'] = last_30_count
        
        time.sleep(wait_time)
    
    return company_details

def main():
    parser = argparse.ArgumentParser(description="LinkedIn Profile and Ad Count Scraper")
    parser.add_argument('--input', default='Untitled spreadsheet - Sheet1 (2).csv', help='Input CSV file with profile URLs')
    parser.add_argument('--output', default='profile_combined_data.csv', help='Output CSV file')
    parser.add_argument('--intermediate', default='profile_data.json', help='Intermediate JSON file for profile data')
    parser.add_argument('--visible', action='store_true', help='Show the browser window during execution')
    parser.add_argument('--wait', type=int, default=2, help='Additional wait time after page load in seconds')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--username', help='LinkedIn username')
    parser.add_argument('--password', help='LinkedIn password')
    parser.add_argument('--skip-profile-fetch', action='store_true', help='Skip profile data fetching and use existing JSON file')
    parser.add_argument('--skip-ad-scrape', action='store_true', help='Skip ad count scraping')
    args = parser.parse_args()
    
    logger = setup_logging()
    logger.setLevel(logging.DEBUG if args.debug else logging.INFO)
    logger.info("Starting LinkedIn Profile and Ad Count Scraper")
    
    company_details = []
    
    # PART 1: Fetch Profile Details using Bright Data API
    if not args.skip_profile_fetch:
        logger.info("Starting profile data fetch from Bright Data API")
        
        profiles = read_profiles(args.input)
        logger.info(f"Processing {len(profiles)} profiles...")
        
        snapshot_id = trigger_collection(profiles, PROFILE_DATASET_ID)
        if not snapshot_id:
            logger.error("Failed to trigger collection")
            sys.exit(1)
        
        if not wait_for_completion(snapshot_id):
            logger.error("Collection failed or timed out")
            sys.exit(1)
        
        results = fetch_results(snapshot_id)
        if results:
            save_results(results, args.intermediate)
            company_details = extract_company_details(results)
            logger.info(f"Extracted details for {len(company_details)} companies")
        else:
            logger.error("Failed to fetch results")
            sys.exit(1)
    else:
        logger.info(f"Loading profile data from {args.intermediate}")
        try:
            with open(args.intermediate, 'r') as f:
                results = json.load(f)
                company_details = extract_company_details(results)
                logger.info(f"Loaded details for {len(company_details)} companies")
        except Exception as e:
            logger.error(f"Error loading profile data: {e}")
            sys.exit(1)
    
    # PART 2: Fetch Ad Counts using Selenium
    if not args.skip_ad_scrape:
        logger.info("Starting ad count scraping")
        
        chrome_options = Options()
        if not args.visible:
            chrome_options.add_argument("--headless=new")
            chrome_options.add_argument("--disable-gpu")
            logger.info("Running in headless mode")
        else:
            logger.info("Running in visible mode")

        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--start-maximized')
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_experimental_option('excludeSwitches', ['enable-automation'])
        chrome_options.add_experimental_option('useAutomationExtension', False)

        logger.info("Initializing Chrome WebDriver")
        try:
            browser = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
        except Exception as e:
            logger.warning(f"Error initializing Chrome with WebDriverManager: {e}")
            logger.info("Trying alternative ChromeDriver initialization method")
            browser = webdriver.Chrome(options=chrome_options)
        
        browser.execute_cdp_cmd('Network.setUserAgentOverride', {
            "userAgent": 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })
        browser.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
            'source': '''
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                })
            '''
        })
        
        browser.set_window_size(1920, 1080)
        
        try:
            login_success = login_to_linkedin(browser, args.username, args.password, logger)
            
            if not login_success:
                logger.warning("LinkedIn login may not have been successful. Results may be affected.")
            else:
                logger.info("LinkedIn login successful, proceeding with ad scraping")
            
            company_details = scrape_ad_counts(company_details, browser, logger, args.wait)
            
        except Exception as e:
            logger.error(f"Error during ad scraping: {e}", exc_info=True)
        
        finally:
            logger.info("Closing browser")
            browser.quit()
    
    # Save combined data to CSV
    logger.info("Saving combined data to CSV")
    try:
        df = pd.DataFrame(company_details)
        df.to_csv(args.output, index=False)
        logger.info(f"Combined results saved to {args.output}")
        
        print("\nSummary of data collected:")
        print(f"Total companies processed: {len(company_details)}")
        print(f"Data saved to: {args.output}")
        
    except Exception as e:
        logger.error(f"Error saving combined data: {e}")

if __name__ == "__main__":
    main() 
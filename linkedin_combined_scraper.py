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
DATASET_ID = "gd_l1vikfnt1wgvvqz95w"
BASE_URL = "https://api.brightdata.com/datasets/v3"

# LinkedIn credentials (default values)
DEFAULT_LINKEDIN_USERNAME = "kvtvpxgaming@gmail.com"
DEFAULT_LINKEDIN_PASSWORD = "Hello@1055"

def setup_logging():
    """Configure basic logging for the script"""
    # Create logs directory
    logs_dir = create_directory("logs")
    
    # Create a timestamp for the log filename
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(logs_dir, f"linkedin_combined_scraper_{timestamp}.log")
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    
    logger = logging.getLogger("linkedin_combined_scraper")
    logger.info(f"Logging initialized. Log file: {log_file}")
    return logger

def create_directory(dir_name):
    """Create a directory if it doesn't exist"""
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)
        logging.info(f"Created directory: {dir_name}")
    return dir_name

# --- Company Data Fetcher Functions ---

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

def extract_company_details(data: Union[List[Dict], Dict]) -> List[Dict]:
    """Extract key company details from the API response."""
    # Handle both list and single company responses
    companies = data if isinstance(data, list) else [data]
    company_details = []
    
    for company in companies:
        if isinstance(company, str):
            # Skip string entries
            continue
            
        # Extract key details
        company_detail = {
            'company_name': company.get('name', 'N/A'),
            'company_id': company.get('company_id', 'N/A'),
            'url': company.get('url', 'N/A'),
            'industry': company.get('industries', 'N/A'),
            'location': company.get('headquarters', 'N/A'),
            'company_size': company.get('company_size', 'N/A'),
            'followers': company.get('followers', 'N/A'),
            'organization_type': company.get('organization_type', 'N/A'),
            'website': company.get('website', 'N/A'),
            'about': company.get('about', 'N/A'),
            'snapshot_date': datetime.datetime.now().strftime("%Y-%m-%d")
        }
        company_details.append(company_detail)
    
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
    """
    Extracts the number of ads from a LinkedIn Ad Library page
    
    Args:
        browser: Selenium WebDriver instance
        url: URL of the LinkedIn Ad Library page to scrape
        logger: Logger instance
        
    Returns:
        int: Number of ads found, 0 if no ads, or None if couldn't determine
    """
    logger.info(f"Navigating to {url}")
    
    try:
        browser.get(url)
        
        # Wait longer for initial page load
        time.sleep(8)
        
        # Scroll down to trigger dynamic loading
        browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        browser.execute_script("window.scrollTo(0, 0);")
        time.sleep(2)
        
        # Capture screenshot to see what's loading
        screenshot_file = f"ad_library_{int(time.time())}.png"
        screenshot_path = capture_screenshot(browser, screenshot_file)
        logger.info(f"Screenshot saved to {screenshot_path}")
        
        # Wait for the ad count element to appear with multiple possible patterns
        logger.info("Waiting for ad count element to load...")
        
        # Try different XPaths that might contain the ad count
        xpath_patterns = [
            # Most specific patterns first
            "//div[contains(@class, 'search-results-container')]//span[contains(text(), 'ads match')]",
            "//div[contains(@class, 'search-results')]//h2[contains(text(), 'ads match')]",
            "//span[contains(@class, 'results-context-header')][contains(text(), 'ads match')]",
            # Original patterns
            "//span[contains(text(), 'ads match') or contains(text(), 'ad matches')]",
            "//div[contains(text(), 'ads match')]",
            # Backup patterns
            "//h1[contains(text(), 'ads match')]",
            "//h2[contains(text(), 'ads match')]",
            "//div[contains(@class, 'search-results')]//span[contains(text(), 'ads')]"
        ]
        
        # First try to find elements with explicit text
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
                            # Try to match the full number including commas
                            match = re.search(r'([\d,]+)\s+ads?\s+match', text)
                            if match:
                                count_str = match.group(1).replace(',', '')
                                count = int(count_str)
                                logger.info(f"Found {count} ads from element text")
                                return count
            except Exception as e:
                logger.warning(f"Error processing XPath {xpath}: {e}")
                continue
        
        # If no explicit elements found, try JavaScript to get computed text
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
        
        # Last resort: Check the full page source
        logger.info("Checking full page source as fallback")
        page_source = browser.page_source
        
        # Look for numbers with commas
        matches = re.findall(r'([\d,]+)\s+ads?\s+match', page_source)
        if matches:
            # Get the largest number found
            counts = [int(m.replace(',', '')) for m in matches]
            count = max(counts)
            logger.info(f"Found {count} ads in page source (largest match)")
            return count
        
        # Check for zero results
        if re.search(r'No ads to show|No results found', page_source, re.IGNORECASE):
            logger.info("No ads found on this page")
            return 0
        
        logger.warning("Could not find ad count on the page")
        return None
            
    except Exception as e:
        logger.error(f"Error loading {url}: {e}")
        return None

def scrape_ad_counts(company_details, browser, logger, wait_time=2):
    """
    Scrape ad counts for companies and add to their details
    
    Args:
        company_details: List of dicts containing company details
        browser: Selenium WebDriver instance
        logger: Logger instance
        wait_time: Time to wait between requests
        
    Returns:
        List of updated company details with ad counts
    """
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
        
        # Add ad counts to company details
        company['all_time_ads'] = all_time_count
        company['last_30_days_ads'] = last_30_count
        
        # Be polite to LinkedIn
        time.sleep(wait_time)
    
    return company_details

def main():
    parser = argparse.ArgumentParser(description="LinkedIn Company Data and Ad Count Scraper")
    parser.add_argument('--input', default='example_companies.csv', help='Input CSV file with company URLs')
    parser.add_argument('--output', default='company_combined_data.csv', help='Output CSV file')
    parser.add_argument('--intermediate', default='company_data.json', help='Intermediate JSON file for company data')
    parser.add_argument('--visible', action='store_true', help='Show the browser window during execution')
    parser.add_argument('--wait', type=int, default=2, help='Additional wait time after page load in seconds')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--username', help='LinkedIn username')
    parser.add_argument('--password', help='LinkedIn password')
    parser.add_argument('--skip-company-fetch', action='store_true', help='Skip company data fetching and use existing JSON file')
    parser.add_argument('--skip-ad-scrape', action='store_true', help='Skip ad count scraping')
    args = parser.parse_args()
    
    # Setup logging
    logger = setup_logging()
    logger.setLevel(logging.DEBUG if args.debug else logging.INFO)
    logger.info("Starting LinkedIn Company Data and Ad Count Scraper")
    
    company_details = []
    
    # PART 1: Fetch Company Details using Bright Data API
    if not args.skip_company_fetch:
        logger.info("Starting company data fetch from Bright Data API")
        
        # Read company URLs from CSV
        logger.info(f"Reading companies from {args.input}...")
        companies = read_companies(args.input)
        logger.info(f"Processing {len(companies)} companies...")
        
        # Trigger collection and get snapshot ID
        snapshot_id = trigger_collection(companies)
        if not snapshot_id:
            logger.error("Failed to trigger collection")
            sys.exit(1)
        
        # Wait for collection to complete
        if not wait_for_completion(snapshot_id):
            logger.error("Collection failed or timed out")
            sys.exit(1)
        
        # Fetch and save results
        results = fetch_results(snapshot_id)
        if results:
            save_results(results, args.intermediate)
            company_details = extract_company_details(results)
            logger.info(f"Extracted details for {len(company_details)} companies")
        else:
            logger.error("Failed to fetch results")
            sys.exit(1)
    else:
        # Load company details from existing JSON file
        logger.info(f"Loading company data from {args.intermediate}")
        try:
            with open(args.intermediate, 'r') as f:
                results = json.load(f)
                company_details = extract_company_details(results)
                logger.info(f"Loaded details for {len(company_details)} companies")
        except Exception as e:
            logger.error(f"Error loading company data: {e}")
            sys.exit(1)
    
    # PART 2: Fetch Ad Counts using Selenium
    if not args.skip_ad_scrape:
        logger.info("Starting ad count scraping")
        
        chrome_options = Options()
        if not args.visible:
            # Modern headless mode configuration
            chrome_options.add_argument("--headless=new")
            chrome_options.add_argument("--disable-gpu")
            logger.info("Running in headless mode")
        else:
            logger.info("Running in visible mode")

        # Common options for both modes
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--start-maximized')
        
        # Add user agent to appear more like a real browser
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        
        # Disable automation flags
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_experimental_option('excludeSwitches', ['enable-automation'])
        chrome_options.add_experimental_option('useAutomationExtension', False)

        logger.info("Initializing Chrome WebDriver")
        try:
            initial_driver_path = ChromeDriverManager().install()
            logger.info(f"Initial ChromeDriver path from WebDriverManager: {initial_driver_path}")

            # Correct the path if it points to the notices file or a directory
            if initial_driver_path.endswith("THIRD_PARTY_NOTICES.chromedriver"):
                logger.warning("WebDriverManager returned path to notices file. Adjusting to chromedriver executable.")
                driver_path = os.path.join(os.path.dirname(initial_driver_path), "chromedriver")
                logger.info(f"Adjusted ChromeDriver path: {driver_path}")
            elif os.path.isdir(initial_driver_path):
                logger.warning("WebDriverManager returned a directory. Attempting to find 'chromedriver' executable within.")
                potential_driver_path = os.path.join(initial_driver_path, "chromedriver")
                if os.path.exists(potential_driver_path) and not os.path.isdir(potential_driver_path):
                    driver_path = potential_driver_path
                    logger.info(f"Adjusted ChromeDriver path from directory: {driver_path}")
                else:
                    logger.error(f"WebDriverManager returned a directory '{initial_driver_path}', but 'chromedriver' executable not found directly within.")
                    driver_path = initial_driver_path # Fallback, likely to fail
            else:
                driver_path = initial_driver_path

            if not os.path.exists(driver_path) or os.path.isdir(driver_path):
                logger.error(f"ChromeDriver executable not found or is a directory at the determined path: {driver_path}.")
                raise Exception(f"ChromeDriver executable not found at {driver_path}")

            logger.info(f"Using ChromeDriver executable at: {driver_path}")
            
            os.chmod(driver_path, 0o755)
            logger.info(f"Set execute permissions for: {driver_path}")
            
            service = Service(executable_path=driver_path)
            browser = webdriver.Chrome(service=service, options=chrome_options)

        except Exception as e:
            logger.error(f"Fatal error initializing Chrome WebDriver: {e}", exc_info=True)
            raise  # Re-raise the exception
        
        # Execute CDP commands to prevent detection
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
        
        # Set window size explicitly
        browser.set_window_size(1920, 1080)
        
        try:
            # Login to LinkedIn first
            login_success = login_to_linkedin(browser, args.username, args.password, logger, visible_mode=args.visible)
            
            if not login_success:
                logger.warning("LinkedIn login may not have been successful. Results may be affected.")
            else:
                logger.info("LinkedIn login successful, proceeding with ad scraping")
            
            # Scrape ad counts for each company
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
        
        # Print summary
        print("\nSummary of data collected:")
        print(f"Total companies processed: {len(company_details)}")
        print(f"Data saved to: {args.output}")
        
    except Exception as e:
        logger.error(f"Error saving combined data: {e}")

if __name__ == "__main__":
    main() 
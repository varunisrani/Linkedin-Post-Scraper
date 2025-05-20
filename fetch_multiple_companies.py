import argparse
import os
import time
import re
import pandas as pd
import logging
import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import requests

# Setup logging
def setup_logging(log_level=logging.INFO):
    """Setup logging configuration with file and console handlers"""
    # Create logs directory
    logs_dir = create_directory("logs")
    
    # Create a timestamp for the log filename
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(logs_dir, f"linkedin_scraper_{timestamp}.log")
    
    # Configure logging
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    
    logging.info(f"Logging initialized. Log file: {log_file}")
    return log_file

def create_directory(dir_name):
    """Create a directory if it doesn't exist"""
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)
        logging.info(f"Created directory: {dir_name}")
    return dir_name

def capture_screenshot(browser, filename):
    """Capture a screenshot of the current page"""
    try:
        screenshots_dir = create_directory("screenshots")
        filepath = os.path.join(screenshots_dir, filename)
        browser.save_screenshot(filepath)
        logging.info(f"Screenshot saved to {filepath}")
        return True
    except Exception as e:
        logging.error(f"Error capturing screenshot: {e}")
        return False

def login_to_linkedin(browser, username=None, password=None):
    """Login to LinkedIn with the provided credentials"""
    if username is None or password is None:
        username = "kvtvpxgaming@gmail.com"
        password = "Hello@1055"
    
    logging.info("Logging in to LinkedIn...")
    browser.get('https://www.linkedin.com/login')
    
    try:
        WebDriverWait(browser, 10).until(EC.presence_of_element_located((By.ID, "username")))
        logging.info("Login page loaded successfully")
        
        elementID = browser.find_element(By.ID, "username")
        elementID.send_keys(username)
        logging.info("Username entered")
        
        elementID = browser.find_element(By.ID, "password")
        elementID.send_keys(password)
        logging.info("Password entered")
        
        elementID.submit()
        logging.info("Login form submitted")
        
        WebDriverWait(browser, 30).until(EC.url_changes('https://www.linkedin.com/login'))
        logging.info("Successfully logged in to LinkedIn")
        
        if "/feed/" in browser.current_url:
            logging.info("Confirmed login success - redirected to LinkedIn feed")
        else:
            logging.warning(f"Redirected to: {browser.current_url}")
            if "checkpoint" in browser.current_url:
                logging.warning("Security checkpoint detected. Waiting for manual verification...")
                time.sleep(30)
                logging.info("Resuming after checkpoint wait")
    except Exception as e:
        logging.error(f"Error during login process: {e}")
        logging.warning("Login process may have failed or took longer than expected, but continuing...")
    
    time.sleep(10)
    capture_screenshot(browser, "linkedin_login_status.png")
    logging.info("Login process completed")
    return True

def get_original_source(driver):
    """Get the original HTML source before JavaScript modifications"""
    try:
        logging.info("Attempting to get original HTML source")
        original_source = driver.execute_script("return document.getElementsByTagName('html')[0].outerHTML")
        logging.info(f"Successfully retrieved original source ({len(original_source)} characters)")
        return original_source
    except Exception as e:
        logging.error(f"Error getting original source: {e}")
        logging.warning("Falling back to regular page source")
        return driver.page_source

def extract_company_id(source_code):
    """Extract LinkedIn company ID from source code"""
    logging.info("Extracting company ID from source code")
    
    # First specifically look for urn:li:fsd_company pattern (priority)
    fsd_pattern = r'urn:li:fsd_company:(\d+)'
    fsd_matches = re.findall(fsd_pattern, source_code)
    if fsd_matches:
        logging.info(f"Found company ID using primary pattern: {fsd_matches[0]}")
        logging.info(f"Pattern: urn:li:fsd_company:{fsd_matches[0]}")
        print("\n" + "="*50)
        print(f"FOUND COMPANY ID: {fsd_matches[0]}")
        print(f"PATTERN: urn:li:fsd_company:{fsd_matches[0]}")
        print("="*50 + "\n")
        return fsd_matches[0]
    
    # Look for other patterns if fsd_company not found
    patterns = [
        r'urn:li:company:(\d+)',
        r'"companyId":(\d+)',
        r'voyagerCompanyId=(\d+)',
        r'f_C=(\d+)'
    ]
    
    for pattern in patterns:
        logging.info(f"Trying alternative pattern: {pattern}")
        matches = re.findall(pattern, source_code)
        if matches:
            logging.info(f"Found company ID using alternative pattern: {matches[0]}")
            
            # Display the pattern with ID
            if 'urn:li:company:' in pattern:
                pattern_with_id = f"urn:li:company:{matches[0]}"
            elif 'companyId' in pattern:
                pattern_with_id = f"companyId:{matches[0]}"
            elif 'voyagerCompanyId' in pattern:
                pattern_with_id = f"voyagerCompanyId={matches[0]}"
            elif 'f_C' in pattern:
                pattern_with_id = f"f_C={matches[0]}"
            else:
                pattern_with_id = f"ID:{matches[0]}"
                
            logging.info(f"Pattern with ID: {pattern_with_id}")
            print("\n" + "="*50)
            print(f"FOUND COMPANY ID: {matches[0]}")
            print(f"PATTERN: {pattern_with_id}")
            print("="*50 + "\n")
            return matches[0]
    
    logging.warning("No company ID found in source code")
    print("\n" + "="*50)
    print("NO COMPANY ID FOUND")
    print("="*50 + "\n")
    return None

def fetch_linkedin_source(url, driver, wait_time=5, save_screenshot=True):
    """Fetch the source code of a LinkedIn page"""
    logging.info(f"Fetching source code from: {url}")
    
    result = {
        "url": url,
        "success": False,
        "source": "",
        "title": "",
        "company_id": None,
        "error": None
    }
    
    try:
        logging.info(f"Navigating to {url}...")
        driver.get(url)
        
        logging.info(f"Waiting {wait_time} seconds for page to load...")
        time.sleep(wait_time)
        
        result["title"] = driver.title
        logging.info(f"Page title: {result['title']}")
        
        if save_screenshot:
            domain = urlparse(url).netloc
            capture_screenshot(driver, f"{domain}_screenshot.png")
        
        logging.info("Getting page source code...")
        result["source"] = get_original_source(driver)
        
        logging.info("Searching source code for company ID...")
        if "company" in url:
            company_id = extract_company_id(result["source"])
            result["company_id"] = company_id
            
            if company_id:
                logging.info(f"Successfully extracted company ID: {company_id}")
                
                # Save the patterns for debugging
                fsd_pattern = f"urn:li:fsd_company:{company_id}"
                company_pattern = f"urn:li:company:{company_id}"
                
                # Find and save context around the pattern
                if fsd_pattern in result["source"]:
                    logging.info("Saving context around the company ID pattern")
                    start_idx = max(0, result["source"].find(fsd_pattern) - 100)
                    end_idx = min(len(result["source"]), result["source"].find(fsd_pattern) + 100)
                    context = result["source"][start_idx:end_idx]
                    
                    context_file = os.path.join("output", f"company_id_context_{company_id}.txt")
                    with open(context_file, "w") as f:
                        f.write(f"Pattern found: {fsd_pattern}\n\nContext:\n{context}")
                    logging.info(f"Saved pattern context to {context_file}")
            else:
                logging.warning(f"No company ID found for {url}")
        
        result["success"] = True
        logging.info(f"Successfully fetched source code from {url}")
        logging.info(f"Source code length: {len(result['source'])} characters")
        
    except Exception as e:
        result["error"] = str(e)
        logging.error(f"Error fetching source code: {e}")
    
    return result

def save_source_code(source_code, url, output_dir="output", company_id=None):
    """Save the source code to a file"""
    logging.info(f"Saving source code from {url}")
    create_directory(output_dir)
    
    domain = urlparse(url).netloc
    filename = f"{domain}_source.html"
    
    # Add company ID to filename if available
    if company_id:
        filename = f"{domain}_company_{company_id}_source.html"
    
    filepath = os.path.join(output_dir, filename)
    
    logging.info(f"Writing source code to {filepath}")
    try:
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(source_code)
        logging.info(f"Source code saved to {filepath}")
    except Exception as e:
        logging.error(f"Error saving source code: {e}")
    
    return filepath

def main():
    parser = argparse.ArgumentParser(description="Fetch source code from multiple LinkedIn URLs")
    parser.add_argument("--csv", default="company_list_sample.csv", help="CSV file with company list")
    parser.add_argument("--wait", type=int, default=5, help="Time to wait for page to load in seconds (default: 5)")
    parser.add_argument("--visible", action="store_true", help="Show the browser window during execution")
    parser.add_argument("--no-screenshot", action="store_true", help="Don't save a screenshot of the page")
    parser.add_argument("--output-dir", default="output", help="Directory to save output files (default: output)")
    parser.add_argument("--username", help="LinkedIn username")
    parser.add_argument("--password", help="LinkedIn password")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = parser.parse_args()
    
    # Setup logging
    log_level = logging.DEBUG if args.debug else logging.INFO
    log_file = setup_logging(log_level)
    
    logging.info(f"Starting LinkedIn company ID scraper")
    logging.info(f"CSV file: {args.csv}")
    logging.info(f"Wait time: {args.wait} seconds")
    logging.info(f"Visible mode: {args.visible}")
    logging.info(f"Output directory: {args.output_dir}")
    
    # Initialize Chrome options
    chrome_options = Options()
    if not args.visible:
        chrome_options.add_argument("--headless")
        logging.info("Running in headless mode")
    else:
        logging.info("Running in visible mode")
    
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    
    try:
        logging.info("Initializing Chrome WebDriver")
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
        logging.info("Chrome WebDriver initialized successfully")
        
        # Login to LinkedIn
        login_to_linkedin(driver, args.username, args.password)
        
        # Read the companies CSV
        try:
            logging.info(f"Reading companies from {args.csv}")
            companies_df = pd.read_csv(args.csv)
            logging.info(f"Found {len(companies_df)} companies in CSV")
        except Exception as e:
            logging.error(f"Error reading CSV file: {e}")
            return
        
        # Create a list to store results
        results = []
        
        # Process each company
        for index, row in companies_df.iterrows():
            company_name = row['company_name']
            company_url = row['company_url']
            
            logging.info(f"\n{'='*30}")
            logging.info(f"Processing company {index+1}/{len(companies_df)}: {company_name}")
            logging.info(f"URL: {company_url}")
            
            # Fetch the source code
            result = fetch_linkedin_source(company_url, driver, args.wait, not args.no_screenshot)
            
            # Save the source code if successful
            if result["success"]:
                save_source_code(result["source"], result["url"], args.output_dir, result["company_id"])
                
                # Log verification URL if company ID was found
                if result["company_id"]:
                    verification_url = f"https://www.linkedin.com/company/{result['company_id']}"
                    logging.info(f"Verify company ID with: {verification_url}")
                    print(f"Verify company ID with: {verification_url}")
            
            # Add to results
            results.append({
                'company_name': company_name,
                'company_url': company_url,
                'company_id': result['company_id']
            })
            
            logging.info(f"Completed processing {company_name}")
            logging.info(f"{'='*30}\n")
        
        # Save results to CSV
        logging.info("Saving results to CSV")
        results_df = pd.DataFrame(results)
        output_csv = os.path.join(args.output_dir, 'company_ids.csv')
        results_df.to_csv(output_csv, index=False)
        logging.info(f"Results saved to {output_csv}")
        
        # Generate a summary
        success_count = sum(1 for r in results if r['company_id'] is not None)
        failed_count = len(results) - success_count
        
        logging.info("\nSCRAPING SUMMARY")
        logging.info(f"Total companies processed: {len(results)}")
        logging.info(f"Successfully found IDs: {success_count}")
        logging.info(f"Failed to find IDs: {failed_count}")
        logging.info(f"Success rate: {success_count/len(results)*100:.2f}%")
        
        print("\nSCRAPING SUMMARY")
        print(f"Total companies processed: {len(results)}")
        print(f"Successfully found IDs: {success_count}")
        print(f"Failed to find IDs: {failed_count}")
        print(f"Success rate: {success_count/len(results)*100:.2f}%")
        print(f"Log file: {log_file}")
        print(f"Results saved to {output_csv}")
        
    except Exception as e:
        logging.error(f"Error in main execution: {e}", exc_info=True)
    
    finally:
        # Close the browser
        if 'driver' in locals():
            logging.info("Closing browser")
            driver.quit()
            logging.info("Browser closed")
        
        logging.info("Script execution completed")

if __name__ == "__main__":
    main() 
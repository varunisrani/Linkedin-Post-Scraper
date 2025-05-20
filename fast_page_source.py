import argparse
import os
import time
import re
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import pandas as pd
import logging
from datetime import datetime

def setup_logging():
    """Configure logging with both file and console output"""
    # Create logs directory
    if not os.path.exists("logs"):
        os.makedirs("logs")
    
    # Create log filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join("logs", f"fast_scraper_{timestamp}.log")
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger()

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
        return filepath
    except Exception as e:
        logging.error(f"Error capturing screenshot: {e}")
        return None

def initialize_driver(headless=True):
    """Initialize and configure Chrome WebDriver"""
    chrome_options = Options()
    if headless:
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--disable-gpu")
    
    # Common options
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--start-maximized")
    
    # Anti-detection measures
    chrome_options.add_argument('--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    chrome_options.add_experimental_option('excludeSwitches', ['enable-automation'])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    
    # Additional anti-detection measures
    driver.execute_cdp_cmd('Network.setUserAgentOverride', {
        "userAgent": 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    })
    driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
        'source': '''
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            })
        '''
    })
    
    return driver

def quick_login(browser, username=None, password=None):
    """Quickly log in to LinkedIn without waiting for feed page or verification"""
    if username is None or password is None:
        username = "kvtvpxgaming@gmail.com"
        password = "Hello@1055"
    
    try:
        # Only load login page once
        browser.get('https://www.linkedin.com/login')
        
        # Enter credentials immediately
        username_field = browser.find_element(By.ID, "username")
        password_field = browser.find_element(By.ID, "password")
        
        username_field.send_keys(username)
        password_field.send_keys(password)
        
        # Click login and proceed immediately
        password_field.submit()
        
        # Minimal wait before proceeding
        time.sleep(2)
        
        return True
        
    except Exception as e:
        logging.error(f"Login error: {e}")
        return False

def force_navigate(browser, url, max_retries=3):
    """Force navigation to URL even if authwall appears"""
    for attempt in range(max_retries):
        try:
            # Try direct navigation
            browser.get(url)
            time.sleep(2)
            
            # Check if we hit authwall
            current_url = browser.current_url
            if "authwall" in current_url:
                logging.warning("Authwall detected, forcing direct company URL...")
                
                # Extract the actual company URL from sessionRedirect parameter if present
                if "sessionRedirect" in current_url:
                    try:
                        from urllib.parse import unquote
                        redirect_url = re.search(r'sessionRedirect=([^&]+)', current_url).group(1)
                        company_url = unquote(redirect_url)
                        url = company_url
                    except:
                        pass
                
                # Force navigate using JavaScript
                browser.execute_script(f'window.location.href = "{url}"')
                time.sleep(2)
                
                # If still on authwall, try direct company URL format
                if "authwall" in browser.current_url and "/company/" in url:
                    company_id = url.split("/company/")[-1].split("/")[0]
                    direct_url = f"https://www.linkedin.com/company/{company_id}"
                    browser.execute_script(f'window.location.href = "{direct_url}"')
                    time.sleep(2)
            
            return True
            
        except Exception as e:
            logging.error(f"Navigation error (attempt {attempt+1}): {e}")
            if attempt == max_retries - 1:
                return False
            time.sleep(1)
    
    return False

def get_page_source(browser, url, wait_time=2):
    """Get page source with minimal waiting"""
    try:
        logging.info(f"Fetching: {url}")
        
        # Force navigation even if security check appears
        if not force_navigate(browser, url):
            logging.warning("Could not navigate to URL, trying to get source anyway")
        
        # Try to get source even if navigation seems failed
        source = browser.execute_script("return document.documentElement.outerHTML")
        
        # Always consider it a success if we got any source
        if source:
            return {
                "success": True,
                "source": source,
                "title": browser.title
            }
        
        return {
            "success": False,
            "source": "",
            "title": "",
            "error": "No source found"
        }
        
    except Exception as e:
        logging.error(f"Error fetching {url}: {e}")
        return {
            "success": False,
            "source": "",
            "title": "",
            "error": str(e)
        }

def extract_company_id(source):
    """Extract LinkedIn company ID from source code"""
    # Priority pattern
    fsd_match = re.search(r'urn:li:fsd_company:(\d+)', source)
    if fsd_match:
        return fsd_match.group(1)
    
    # Fallback patterns
    patterns = [
        r'urn:li:company:(\d+)',
        r'"companyId":(\d+)',
        r'voyagerCompanyId=(\d+)',
        r'f_C=(\d+)'
    ]
    
    for pattern in patterns:
        match = re.search(pattern, source)
        if match:
            return match.group(1)
    
    return None

def save_source(source, url, company_id=None, output_dir="output"):
    """Save source code to file"""
    try:
        create_directory(output_dir)
        
        domain = urlparse(url).netloc
        filename = f"{domain}_company_{company_id}_source.html" if company_id else f"{domain}_source.html"
        filepath = os.path.join(output_dir, filename)
        
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(source)
            
        logging.info(f"Saved source to: {filepath}")
        return filepath
    except Exception as e:
        logging.error(f"Error saving source: {e}")
        return None

def process_company_urls(urls, headless=True, output_dir="output", username=None, password=None):
    """Process multiple company URLs efficiently"""
    logger = setup_logging()
    logger.info(f"Processing {len(urls)} URLs")
    
    results = []
    browser = initialize_driver(headless)
    
    try:
        # Do login only once at the start
        quick_login(browser, username, password)
        
        # Process each URL
        for url in urls:
            result = {
                "url": url,
                "company_id": None,
                "source_file": None,
                "success": False
            }
            
            try:
                # Get page source (with forced navigation)
                page_data = get_page_source(browser, url)
                
                # Try to extract company ID even if page seems not loaded
                if page_data["source"]:
                    company_id = extract_company_id(page_data["source"])
                    result["company_id"] = company_id
                    
                    if company_id:
                        source_file = save_source(page_data["source"], url, company_id, output_dir)
                        result["source_file"] = source_file
                        result["success"] = True
                        
                        # Take quick screenshot
                        capture_screenshot(browser, f"company_{company_id}.png")
                
                results.append(result)
                logger.info(f"Processed {url} - ID: {result['company_id']}")
                
                # Minimal wait between requests
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Error processing URL {url}: {e}")
                result["error"] = str(e)
                results.append(result)
                continue
    
    except Exception as e:
        logger.error(f"Error during processing: {e}")
    
    finally:
        browser.quit()
        logger.info("Browser closed")
    
    return results

def main():
    parser = argparse.ArgumentParser(description="Fast LinkedIn page source scraper")
    parser.add_argument("--input", help="CSV file with company URLs (company_url column)")
    parser.add_argument("--url", help="Single company URL to process")
    parser.add_argument("--output-dir", default="output", help="Output directory")
    parser.add_argument("--visible", action="store_true", help="Show browser window")
    parser.add_argument("--username", help="LinkedIn username")
    parser.add_argument("--password", help="LinkedIn password")
    
    args = parser.parse_args()
    
    if not args.input and not args.url:
        print("Error: Must provide either --input or --url")
        return
    
    # Get URLs to process
    urls = []
    if args.input:
        df = pd.read_csv(args.input)
        urls = df["company_url"].tolist()
    else:
        urls = [args.url]
    
    # Process URLs
    results = process_company_urls(
        urls,
        headless=not args.visible,
        output_dir=args.output_dir,
        username=args.username,
        password=args.password
    )
    
    # Save results
    results_df = pd.DataFrame(results)
    results_file = os.path.join(args.output_dir, "scraping_results.csv")
    results_df.to_csv(results_file, index=False)
    print(f"Results saved to: {results_file}")

if __name__ == "__main__":
    main() 
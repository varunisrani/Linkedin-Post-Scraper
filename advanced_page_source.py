import argparse
import os
import time
import re
import random
import json
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from urllib.parse import urlparse, unquote
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
    log_file = os.path.join("logs", f"advanced_scraper_{timestamp}.log")
    
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

def initialize_driver(headless=True, user_data_dir=None, proxy=None):
    """Initialize and configure Chrome WebDriver with advanced anti-detection"""
    chrome_options = Options()
    if headless:
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--disable-gpu")
    
    # Common options
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--start-maximized")
    
    # Randomized user agent from popular browsers
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.2210.133',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    ]
    user_agent = random.choice(user_agents)
    chrome_options.add_argument(f'--user-agent={user_agent}')
    
    # Advanced anti-detection measures
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    chrome_options.add_experimental_option('excludeSwitches', ['enable-automation'])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    
    # Add language and locale settings to appear more like a real user
    chrome_options.add_argument('--lang=en-US,en;q=0.9')
    chrome_options.add_argument('--accept-lang=en-US,en;q=0.9')
    
    # Use existing profile if provided
    if user_data_dir:
        chrome_options.add_argument(f"--user-data-dir={user_data_dir}")
    
    # Add proxy if provided
    if proxy:
        chrome_options.add_argument(f'--proxy-server={proxy}')
    
    # Create driver
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    
    # Additional anti-detection measures using CDP
    driver.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": user_agent})
    
    # Disable webdriver flag
    driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
        'source': '''
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            
            // Hide automation flags
            const originalQuery = window.navigator.permissions.query;
            window.navigator.permissions.query = (parameters) => (
                parameters.name === 'notifications' || parameters.name === 'geolocation' || 
                parameters.name === 'midi' || parameters.name === 'camera' || 
                parameters.name === 'microphone' || parameters.name === 'background-sync' ?
                Promise.resolve({state: Notification.permission, onchange: null}) :
                originalQuery(parameters)
            );
            
            // Override plugins length
            Object.defineProperty(navigator, 'plugins', {
                get: () => {
                    const plugins = {};
                    plugins.__proto__ = {length: 5};
                    return plugins;
                }
            });
            
            // Override language/platform properties
            Object.defineProperty(navigator, 'languages', {
                get: () => ['en-US', 'en']
            });
        '''
    })
    
    return driver

def save_cookies(driver, filename="linkedin_cookies.json"):
    """Save session cookies to file"""
    try:
        cookies_dir = create_directory("cookies")
        filepath = os.path.join(cookies_dir, filename)
        cookies = driver.get_cookies()
        with open(filepath, 'w') as file:
            json.dump(cookies, file)
        logging.info(f"Cookies saved to {filepath}")
        return True
    except Exception as e:
        logging.error(f"Error saving cookies: {e}")
        return False

def load_cookies(driver, filename="linkedin_cookies.json"):
    """Load cookies from file"""
    try:
        filepath = os.path.join("cookies", filename)
        if not os.path.exists(filepath):
            logging.warning(f"Cookie file not found: {filepath}")
            return False
            
        with open(filepath, 'r') as file:
            cookies = json.load(file)
            
        # Visit the domain first
        driver.get("https://www.linkedin.com")
        time.sleep(1)
        
        # Add each cookie
        for cookie in cookies:
            try:
                driver.add_cookie(cookie)
            except Exception as e:
                logging.warning(f"Couldn't add cookie: {e}")
        
        logging.info("Cookies loaded successfully")
        return True
    except Exception as e:
        logging.error(f"Error loading cookies: {e}")
        return False

def advanced_login(driver, username=None, password=None, use_cookies=True):
    """Enhanced login with cookie support and security check bypass"""
    if username is None or password is None:
        username = "kvtvpxgaming@gmail.com"
        password = "Hello@1055"
    
    # Try to use cookies first if enabled
    if use_cookies and load_cookies(driver):
        # Test if we're already logged in
        driver.get("https://www.linkedin.com/feed/")
        time.sleep(2)
        
        # Check if we're on the feed page
        if "/feed" in driver.current_url and "login" not in driver.current_url and "authwall" not in driver.current_url:
            logging.info("Successfully logged in using cookies")
            return True
    
    # If cookies failed or not used, perform manual login
    try:
        logging.info("Performing manual login...")
        driver.get('https://www.linkedin.com/login')
        time.sleep(1)
        
        # Check if we're already on the feed (rare but possible)
        if "/feed" in driver.current_url:
            logging.info("Already logged in")
            if use_cookies:
                save_cookies(driver)
            return True
        
        # Enter credentials
        try:
            username_field = driver.find_element(By.ID, "username")
            password_field = driver.find_element(By.ID, "password")
        except:
            logging.error("Couldn't find login fields")
            return False
            
        # Type like a human with random delays
        for char in username:
            username_field.send_keys(char)
            time.sleep(random.uniform(0.05, 0.15))  # Random delay between keystrokes
            
        for char in password:
            password_field.send_keys(char)
            time.sleep(random.uniform(0.05, 0.15))
            
        # Submit form
        password_field.submit()
        
        # Short wait before checking result
        time.sleep(2)
        
        # Save cookies if login was successful
        if "checkpoint" not in driver.current_url and "authwall" not in driver.current_url and "login" not in driver.current_url:
            if use_cookies:
                save_cookies(driver)
            return True
            
        # Handle security checkpoint if needed
        if "checkpoint" in driver.current_url:
            logging.warning("Security checkpoint detected, attempting bypass...")
            try:
                # Try to find the "Remember" checkbox if exists
                remember_checkbox = driver.find_elements(By.CSS_SELECTOR, "input[type='checkbox']")
                if remember_checkbox:
                    remember_checkbox[0].click()
                    time.sleep(1)
                
                # Try to find and click "I'm not a robot" or similar buttons
                buttons = driver.find_elements(By.TAG_NAME, "button")
                for button in buttons:
                    if "confirm" in button.text.lower() or "verify" in button.text.lower() or "continue" in button.text.lower():
                        button.click()
                        time.sleep(3)
                        break
            except:
                logging.warning("Couldn't bypass automatic checkpoint, may need manual intervention")
                
            # Take screenshot for manual inspection
            capture_screenshot(driver, "security_checkpoint.png")
            
        return "login" not in driver.current_url and "authwall" not in driver.current_url
        
    except Exception as e:
        logging.error(f"Login error: {e}")
        return False

def bypass_authwall(driver, target_url):
    """Bypasses LinkedIn authwall"""
    if "authwall" in driver.current_url:
        logging.warning("Authwall detected, attempting bypass...")
        
        # Extract redirect URL if present
        if "sessionRedirect" in driver.current_url:
            try:
                redirect_url = re.search(r'sessionRedirect=([^&]+)', driver.current_url).group(1)
                actual_url = unquote(redirect_url)
                logging.info(f"Extracted redirect URL: {actual_url}")
                
                # Try direct navigation
                driver.get(actual_url)
                time.sleep(2)
            except:
                logging.warning("Failed to extract redirect URL")
        
        # If still on authwall, use direct company URL pattern
        if "authwall" in driver.current_url and "/company/" in target_url:
            try:
                company_id = target_url.split("/company/")[-1].split("/")[0].split("?")[0]
                direct_url = f"https://www.linkedin.com/company/{company_id}/"
                logging.info(f"Trying direct company URL: {direct_url}")
                
                # Use JavaScript for direct navigation
                driver.execute_script(f'window.location.href = "{direct_url}";')
                time.sleep(2)
            except:
                logging.warning("Failed to extract company ID")
    
        # If still on authwall as last resort, try forcing feed
        if "authwall" in driver.current_url:
            driver.get("https://www.linkedin.com/feed/")
            time.sleep(2)
            driver.get(target_url)
            time.sleep(2)
        
    return "authwall" not in driver.current_url

def get_page_source_advanced(driver, url, retries=3, wait_time=2):
    """Advanced page source extraction with retry mechanism and authwall bypass"""
    result = {
        "success": False,
        "source": "",
        "title": "",
        "error": None
    }
    
    # Multiple retry attempts
    for attempt in range(retries):
        try:
            logging.info(f"Fetching URL (attempt {attempt+1}/{retries}): {url}")
            
            # Navigate to URL
            driver.get(url)
            time.sleep(wait_time)
            
            # Check for authwall and try to bypass
            if "authwall" in driver.current_url:
                bypass_authwall(driver, url)
            
            # Handle forced login
            if "login" in driver.current_url and "authwall" not in driver.current_url:
                logging.warning("Redirected to login page, attempting login...")
                advanced_login(driver)
                driver.get(url)  # Try again
                time.sleep(wait_time)
                
                # Second authwall check
                if "authwall" in driver.current_url:
                    bypass_authwall(driver, url)
            
            # Scroll to load dynamic content
            try:
                # Scroll to bottom
                driver.execute_script("window.scrollTo(0, Math.floor(document.body.scrollHeight/2));")
                time.sleep(1)
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1)
                # Scroll back up
                driver.execute_script("window.scrollTo(0, 0);")
            except:
                logging.warning("Scrolling failed, continuing anyway")
            
            # Get page source
            source = driver.execute_script("return document.documentElement.outerHTML")
            
            if source:
                result["success"] = True
                result["source"] = source
                result["title"] = driver.title
                return result
                
            # If no source but no error, try again
            if attempt < retries - 1:
                logging.warning(f"Empty source on attempt {attempt+1}, retrying...")
                time.sleep(wait_time)
                continue
                
        except Exception as e:
            logging.error(f"Error during attempt {attempt+1}: {e}")
            if attempt < retries - 1:
                logging.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                result["error"] = str(e)
    
    return result

def extract_company_id(source):
    """Extract LinkedIn company ID from source code with enhanced pattern matching"""
    # All potential patterns in order of reliability
    patterns = [
        r'urn:li:fsd_company:(\d+)',  # Most reliable
        r'urn:li:company:(\d+)',
        r'"companyId":(\d+)',
        r'"id":"(\d+)"[^}]*"url":"https:\\/\\/www.linkedin.com\\/company\\/[^"]+"',
        r'voyagerCompanyId=(\d+)',
        r'companyId=(\d+)',
        r'normalized_company:(\d+)',
        r'company:(\d+)',
        r'f_C=(\d+)',
        r'miniCompany:"(\d+)"',
        r'company/(\d+)',  # Last resort - may give false positives
    ]
    
    for pattern in patterns:
        matches = re.findall(pattern, source)
        if matches:
            # Take first match
            company_id = matches[0]
            logging.info(f"Found company ID: {company_id} using pattern: {pattern}")
            return company_id
    
    logging.warning("No company ID found")
    return None

def save_source(source, url, company_id=None, output_dir="output"):
    """Save source code to file with compression option for large files"""
    try:
        create_directory(output_dir)
        
        domain = urlparse(url).netloc
        filename = f"{domain}_company_{company_id}_source.html" if company_id else f"{domain}_source.html"
        filepath = os.path.join(output_dir, filename)
        
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(source)
            
        logging.info(f"Saved source to: {filepath}")
        
        # If file is very large, create compressed version too
        file_size = os.path.getsize(filepath) / (1024 * 1024)  # Size in MB
        if file_size > 5:
            import gzip
            compressed_path = f"{filepath}.gz"
            with open(filepath, 'rb') as f_in:
                with gzip.open(compressed_path, 'wb') as f_out:
                    f_out.write(f_in.read())
            logging.info(f"Created compressed version at: {compressed_path}")
        
        return filepath
    except Exception as e:
        logging.error(f"Error saving source: {e}")
        return None

def process_urls(urls, output_dir="output", headless=True, username=None, password=None, 
                use_cookies=True, proxy=None, user_data_dir=None):
    """Process multiple URLs with enhanced session management"""
    logger = setup_logging()
    logger.info(f"Processing {len(urls)} URLs with advanced techniques")
    
    results = []
    driver = initialize_driver(headless=headless, proxy=proxy, user_data_dir=user_data_dir)
    
    try:
        # Login once at the beginning
        login_success = advanced_login(driver, username, password, use_cookies)
        
        if not login_success:
            logger.warning("Login may have failed, but continuing anyway")
        
        # Process each URL
        for index, url in enumerate(urls):
            try:
                logger.info(f"Processing URL {index+1}/{len(urls)}: {url}")
                
                result = {
                    "url": url,
                    "company_id": None,
                    "source_file": None,
                    "success": False
                }
                
                # Fetch page with advanced techniques
                page_data = get_page_source_advanced(driver, url)
                
                # Process result
                if page_data["success"] and page_data["source"]:
                    # Extract company ID
                    company_id = extract_company_id(page_data["source"])
                    result["company_id"] = company_id
                    
                    # Save source
                    if company_id:
                        source_file = save_source(page_data["source"], url, company_id, output_dir)
                        result["source_file"] = source_file
                        result["success"] = True
                        
                        # Take screenshot
                        capture_screenshot(driver, f"company_{company_id}_{index}.png")
                    else:
                        # Save source even without company ID
                        source_file = save_source(page_data["source"], url, None, output_dir)
                        result["source_file"] = source_file
                
                results.append(result)
                logger.info(f"Completed URL {index+1}: ID={result['company_id']}, Success={result['success']}")
                
                # Variable delay between requests to avoid detection
                delay = random.uniform(1.5, 4.0)
                time.sleep(delay)
                
            except Exception as e:
                logger.error(f"Error processing URL {url}: {e}")
                result = {"url": url, "error": str(e), "success": False}
                results.append(result)
    
    except Exception as e:
        logger.error(f"Fatal error during processing: {e}")
    
    finally:
        # Final cookie save before closing
        if use_cookies:
            save_cookies(driver)
        driver.quit()
        logger.info("Browser closed")
    
    return results

def main():
    parser = argparse.ArgumentParser(description="Advanced LinkedIn page source scraper")
    parser.add_argument("--input", help="CSV file with company URLs (company_url column)")
    parser.add_argument("--url", help="Single company URL to process")
    parser.add_argument("--output-dir", default="output", help="Output directory")
    parser.add_argument("--visible", action="store_true", help="Show browser window")
    parser.add_argument("--username", help="LinkedIn username")
    parser.add_argument("--password", help="LinkedIn password")
    parser.add_argument("--no-cookies", action="store_true", help="Disable cookie saving/loading")
    parser.add_argument("--proxy", help="Proxy server (format: http://user:pass@host:port)")
    parser.add_argument("--profile-dir", help="Chrome user data directory for persistent session")
    
    args = parser.parse_args()
    
    if not args.input and not args.url:
        print("Error: Must provide either --input or --url")
        return
    
    # Get URLs to process
    urls = []
    if args.input:
        try:
            df = pd.read_csv(args.input)
            urls = df["company_url"].tolist()
        except Exception as e:
            print(f"Error reading input file: {e}")
            return
    else:
        urls = [args.url]
    
    # Process URLs with all options
    results = process_urls(
        urls,
        output_dir=args.output_dir,
        headless=not args.visible,
        username=args.username,
        password=args.password,
        use_cookies=not args.no_cookies,
        proxy=args.proxy,
        user_data_dir=args.profile_dir
    )
    
    # Save results
    results_df = pd.DataFrame(results)
    results_file = os.path.join(args.output_dir, "scraping_results.csv")
    results_df.to_csv(results_file, index=False)
    print(f"Results saved to: {results_file}")
    
    # Provide summary
    success_count = sum(1 for r in results if r.get("success", False))
    print(f"Processed {len(urls)} URLs with {success_count} successes")

if __name__ == "__main__":
    main() 
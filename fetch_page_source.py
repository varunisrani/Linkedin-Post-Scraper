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
import requests

def create_directory(dir_name):
    """Create a directory if it doesn't exist"""
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)
        print(f"Created directory: {dir_name}")
    return dir_name

def capture_screenshot(browser, filename):
    """Capture a screenshot of the current page"""
    try:
        screenshots_dir = create_directory("screenshots")
        filepath = os.path.join(screenshots_dir, filename)
        
        browser.save_screenshot(filepath)
        print(f"Screenshot saved to {filepath}")
        return True
    except Exception as e:
        print(f"Error capturing screenshot: {e}")
        return False

def login_to_linkedin(browser, username=None, password=None):
    """Login to LinkedIn with the provided credentials"""
    # Default credentials if none provided
    if username is None or password is None:
        username = "eaglesurvivor12@gmail.com"
        password = "Hello@1055"
        
    print("Logging in to LinkedIn...")
    browser.get('https://www.linkedin.com/login')

    # Wait for the login page to load
    WebDriverWait(browser, 10).until(EC.presence_of_element_located((By.ID, "username")))

    # Enter login credentials and submit
    elementID = browser.find_element(By.ID, "username")
    elementID.send_keys(username)
    elementID = browser.find_element(By.ID, "password")
    elementID.send_keys(password)
    elementID.submit()

    # Wait for login to complete
    try:
        WebDriverWait(browser, 30).until(EC.url_changes('https://www.linkedin.com/login'))
        print("Successfully logged in to LinkedIn")
        
        # Check if we're redirected to the feed page, which indicates successful login
        if "/feed/" in browser.current_url:
            print("Confirmed login success - redirected to LinkedIn feed")
        else:
            print(f"Redirected to: {browser.current_url}")
            
            # Check if we need to handle security verification
            if "checkpoint" in browser.current_url:
                print("Security checkpoint detected. Waiting for manual verification...")
                # Wait longer for manual intervention
                time.sleep(30)
    except:
        print("Login process took longer than expected, but continuing...")
    
    # Wait after login to ensure session is established
    time.sleep(10)
    
    # Take a screenshot to verify login status
    capture_screenshot(browser, "linkedin_login_status.png")
    
    return True

def get_original_source(driver):
    """Get the original HTML source before JavaScript modifications"""
    try:
        # This gets the raw HTML source as it was received from the server
        original_source = driver.execute_script("return document.getElementsByTagName('html')[0].outerHTML")
        return original_source
    except Exception as e:
        print(f"Error getting original source: {e}")
        return driver.page_source  # Fallback to regular page source

def extract_company_id(source_code):
    """Extract LinkedIn company ID from source code"""
    # First specifically look for urn:li:fsd_company pattern (priority)
    fsd_pattern = r'urn:li:fsd_company:(\d+)'
    fsd_matches = re.findall(fsd_pattern, source_code)
    if fsd_matches:
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
        matches = re.findall(pattern, source_code)
        if matches:
            print("\n" + "="*50)
            print(f"FOUND COMPANY ID: {matches[0]}")
            
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
                
            print(f"PATTERN: {pattern_with_id}")
            print("="*50 + "\n")
            return matches[0]
    
    print("\n" + "="*50)
    print("NO COMPANY ID FOUND")
    print("="*50 + "\n")
    return None

def fetch_linkedin_source(url, wait_time=5, headless=True, save_screenshot=True, login_required=True, username=None, password=None):
    """
    Fetch the source code of a LinkedIn page
    
    Args:
        url (str): The LinkedIn URL to fetch
        wait_time (int): Time to wait for page to load in seconds
        headless (bool): Whether to run browser in headless mode
        save_screenshot (bool): Whether to save a screenshot of the page
        login_required (bool): Whether to log in to LinkedIn before visiting URL
        username (str): LinkedIn username
        password (str): LinkedIn password
        
    Returns:
        dict: Contains page source, page title, company ID, and status
    """
    # Initialize Chrome options
    chrome_options = Options()
    if headless:
        chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    
    # Initialize the driver
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    
    result = {
        "url": url,
        "success": False,
        "source": "",
        "title": "",
        "company_id": None,
        "error": None
    }
    
    try:
        # Log in if required
        if login_required and "linkedin.com" in url:
            login_to_linkedin(driver, username, password)
        
        print(f"Navigating to {url}...")
        driver.get(url)
        
        # Wait for the page to load
        time.sleep(wait_time)
        
        # Get the page title
        result["title"] = driver.title
        
        # Take a screenshot if requested
        if save_screenshot:
            domain = urlparse(url).netloc
            capture_screenshot(driver, f"{domain}_screenshot.png")
        
        # Get the original source code
        result["source"] = get_original_source(driver)
        
        # Search source code for relevant company info
        print("Searching source code for company ID...")
        
        # Extract company ID if it's a company page
        if "company" in url:
            company_id = extract_company_id(result["source"])
            result["company_id"] = company_id
            
            # Save the patterns for debugging
            if company_id:
                # Save a small section of HTML containing the ID pattern
                fsd_pattern = f"urn:li:fsd_company:{company_id}"
                company_pattern = f"urn:li:company:{company_id}"
                
                # Find and save context around the pattern
                if fsd_pattern in result["source"]:
                    start_idx = max(0, result["source"].find(fsd_pattern) - 100)
                    end_idx = min(len(result["source"]), result["source"].find(fsd_pattern) + 100)
                    context = result["source"][start_idx:end_idx]
                    
                    context_file = os.path.join("output", f"company_id_context_{company_id}.txt")
                    with open(context_file, "w") as f:
                        f.write(f"Pattern found: {fsd_pattern}\n\nContext:\n{context}")
                    print(f"Saved pattern context to {context_file}")
        
        result["success"] = True
        
        print(f"Successfully fetched source code from {url}")
        print(f"Page title: {result['title']}")
        print(f"Source code length: {len(result['source'])} characters")
        
    except Exception as e:
        result["error"] = str(e)
        print(f"Error fetching source code: {e}")
    
    finally:
        # Close the browser
        driver.quit()
        print("Browser closed")
    
    return result

def save_source_code(source_code, url, output_dir="output", company_id=None):
    """Save the source code to a file"""
    create_directory(output_dir)
    
    domain = urlparse(url).netloc
    filename = f"{domain}_source.html"
    
    # Add company ID to filename if available
    if company_id:
        filename = f"{domain}_company_{company_id}_source.html"
    
    filepath = os.path.join(output_dir, filename)
    
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(source_code)
    
    print(f"Source code saved to {filepath}")
    return filepath

def main():
    parser = argparse.ArgumentParser(description="Fetch source code from LinkedIn URLs")
    parser.add_argument("url", help="LinkedIn URL to fetch source code from")
    parser.add_argument("--wait", type=int, default=5, help="Time to wait for page to load in seconds (default: 5)")
    parser.add_argument("--visible", action="store_true", help="Show the browser window during execution")
    parser.add_argument("--no-screenshot", action="store_true", help="Don't save a screenshot of the page")
    parser.add_argument("--output-dir", default="output", help="Directory to save output files (default: output)")
    parser.add_argument("--no-login", action="store_true", help="Skip LinkedIn login")
    parser.add_argument("--username", help="LinkedIn username")
    parser.add_argument("--password", help="LinkedIn password")
    
    args = parser.parse_args()
    
    # Fetch the source code
    result = fetch_linkedin_source(
        args.url,
        wait_time=args.wait,
        headless=not args.visible,
        save_screenshot=not args.no_screenshot,
        login_required=not args.no_login,
        username=args.username,
        password=args.password
    )
    
    # Save the source code if successful
    if result["success"]:
        save_source_code(
            result["source"], 
            result["url"], 
            args.output_dir, 
            result["company_id"]
        )
        
        # If company ID was found, print verification URL
        if result["company_id"]:
            print(f"Verify company ID with: https://www.linkedin.com/company/{result['company_id']}")
    else:
        print(f"Failed to fetch source code. Error: {result['error']}")

if __name__ == "__main__":
    main()s
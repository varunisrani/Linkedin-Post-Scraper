import time
import re
import argparse
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def get_linkedin_company_id_selenium(company_name):
    # Set up Chrome options
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Run in headless mode
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    
    # Initialize the driver with WebDriverManager to handle ChromeDriver version
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    
    try:
        # Format the LinkedIn company URL properly
        company_url = f"https://www.linkedin.com/company/{company_name}/"
        
        # Navigate to the company page
        driver.get(company_url)
        time.sleep(3)  # Wait for page to load
        
        # Get the page source after JavaScript has loaded
        page_source = driver.page_source
        
        # Look for company ID patterns
        patterns = [
            r'urn:li:fsd_company:(\d+)',
            r'urn:li:company:(\d+)',
            r'"companyId":(\d+)',
            r'f_C=(\d+)'
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, page_source)
            if matches:
                return matches[0]
        
        # If patterns not found, try to find and click on "See jobs" button
        try:
            see_jobs_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//a[contains(text(), 'See jobs') or contains(text(), 'See all jobs')]"))
            )
            
            # Get the href attribute before clicking
            href = see_jobs_button.get_attribute('href')
            if href and 'f_C=' in href:
                match = re.search(r'f_C=(\d+)', href)
                if match:
                    return match.group(1)
                
            # If we couldn't extract from href, try clicking and checking URL
            see_jobs_button.click()
            time.sleep(2)
            
            # Check the URL for company ID
            current_url = driver.current_url
            match = re.search(r'f_C=(\d+)', current_url)
            if match:
                return match.group(1)
        except:
            pass
        
        return "Company ID not found. Try checking network requests manually."
    
    finally:
        # Close the browser
        driver.quit()

def main():
    parser = argparse.ArgumentParser(description='Extract LinkedIn Company ID from company name')
    parser.add_argument('company_name', help='LinkedIn company name (as it appears in the URL)')
    
    args = parser.parse_args()
    company_name = args.company_name
    
    print(f"Fetching company ID from: https://www.linkedin.com/company/{company_name}/")
    company_id = get_linkedin_company_id_selenium(company_name)
    print(f"Company ID: {company_id}")
    
    if company_id.isdigit():
        print(f"Verify with: https://www.linkedin.com/company/{company_id}")

if __name__ == "__main__":
    main()

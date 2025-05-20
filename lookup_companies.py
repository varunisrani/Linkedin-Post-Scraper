from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import time
import argparse
import os
import json
from datetime import datetime

def setup_browser():
    """Set up and return a Chrome browser instance."""
    chrome_options = Options()
    chrome_options.binary_location = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    
    browser = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    return browser

def login_to_linkedin(browser, username, password):
    """Login to LinkedIn with the provided credentials."""
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
    except:
        print("Login process took longer than expected, but continuing...")

def get_company_name(browser, company_id):
    """Extract company name from LinkedIn Ad Library page."""
    url = f"https://www.linkedin.com/ad-library/search?companyIds={company_id}"
    browser.get(url)
    
    # Wait for page to load
    time.sleep(3)
    
    try:
        # Look for company name in the page title or header
        WebDriverWait(browser, 10).until(
            EC.presence_of_any_element_located((By.TAG_NAME, "h1"), (By.TAG_NAME, "h2"))
        )
        
        # Try multiple methods to find company name
        possible_elements = [
            browser.find_elements(By.CSS_SELECTOR, ".ad-library-banner-container h1"),
            browser.find_elements(By.CSS_SELECTOR, ".company-name"),
            browser.find_elements(By.CSS_SELECTOR, ".org-top-card-summary__title"),
            browser.find_elements(By.TAG_NAME, "h1"),
            browser.find_elements(By.XPATH, "//h1[contains(@class, 'company')]"),
            browser.find_elements(By.XPATH, "//h2[contains(@class, 'company')]")
        ]
        
        for elements in possible_elements:
            if elements and elements[0].text.strip():
                # Filter out unwanted text like "LinkedIn Ads for..." or "Ad Library"
                name = elements[0].text.strip()
                name = name.replace("LinkedIn Ads for", "").replace("Ad Library", "").strip()
                if name:
                    return name
        
        # If we can't find the name, extract from page title
        title = browser.title
        if "LinkedIn" in title and " | " in title:
            parts = title.split(" | ")
            name = parts[0].strip()
            if name and name != "LinkedIn" and "Ad Library" not in name:
                return name
                
        return "Unknown"
    except Exception as e:
        print(f"Error getting company name for ID {company_id}: {e}")
        return "Error"

def main():
    parser = argparse.ArgumentParser(description='Lookup LinkedIn company names by ID')
    parser.add_argument('input_file', help='CSV file with company IDs')
    parser.add_argument('--username', '-u', required=True, help='LinkedIn username')
    parser.add_argument('--password', '-p', required=True, help='LinkedIn password')
    parser.add_argument('--output', '-o', default='company_lookup_results.csv', help='Output CSV file')
    parser.add_argument('--limit', '-l', type=int, default=50, help='Maximum number of companies to lookup (default: 50)')
    
    args = parser.parse_args()
    
    # Load company IDs
    try:
        df = pd.read_csv(args.input_file)
        company_ids = df['company_id'].tolist()[:args.limit]
        print(f"Loaded {len(company_ids)} company IDs from {args.input_file}")
    except Exception as e:
        print(f"Error loading input file: {e}")
        return
    
    # Setup browser and login
    browser = setup_browser()
    try:
        login_to_linkedin(browser, args.username, args.password)
        
        # Create results dataframe
        results = []
        
        # Process each company ID
        for i, company_id in enumerate(company_ids, 1):
            print(f"Looking up company {i}/{len(company_ids)}: ID {company_id}")
            company_name = get_company_name(browser, company_id)
            
            frequency = df[df['company_id'] == company_id]['frequency'].values[0] if 'frequency' in df.columns else None
            
            results.append({
                'company_id': company_id,
                'company_name': company_name,
                'frequency': frequency,
                'url': f"https://www.linkedin.com/ad-library/search?companyIds={company_id}"
            })
            
            # Save progress after each lookup
            if i % 5 == 0 or i == len(company_ids):
                results_df = pd.DataFrame(results)
                results_df.to_csv(args.output, index=False)
                print(f"Progress saved to {args.output} ({i}/{len(company_ids)} companies)")
            
            # Pause between requests to avoid rate limiting
            time.sleep(2)
        
        print(f"\nResults saved to {args.output}")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        browser.quit()

if __name__ == "__main__":
    main() 
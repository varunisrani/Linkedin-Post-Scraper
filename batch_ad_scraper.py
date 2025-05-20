#!/usr/bin/env python3
# Batch LinkedIn Ad Scraper
# This script scrapes LinkedIn Ad Library for multiple companies from a CSV file.

# Import necessary libraries
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import pandas as pd
import argparse
import os
import json
from datetime import datetime

# Import scraping functions from the linkedin-ad-scraper script
from linkedin_ad_scraper import (
    login_to_linkedin, 
    create_directory, 
    capture_screenshot, 
    scrape_ad_library
)

def main():
    """Main function to run the batch LinkedIn Ad scraper"""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Batch LinkedIn Ad Library Scraper')
    parser.add_argument('input_file', help='CSV file with company IDs and names')
    parser.add_argument('--username', '-u', required=True, help='LinkedIn username')
    parser.add_argument('--password', '-p', required=True, help='LinkedIn password')
    parser.add_argument('--limit', '-l', type=int, default=10, help='Maximum number of companies to process (default: 10)')
    parser.add_argument('--start', '-s', type=int, default=0, help='Start index (to resume from a specific position)')
    parser.add_argument('--output_dir', '-o', default='batch_results', help='Output directory')
    
    args = parser.parse_args()
    
    # Initialize Chrome options
    chrome_options = Options()
    chrome_options.binary_location = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    
    # Create output directory
    batch_output_dir = create_directory(args.output_dir)
    results_summary = []
    
    try:
        # Load company data
        companies_df = pd.read_csv(args.input_file)
        
        # Ensure required columns exist
        if 'company_id' not in companies_df.columns:
            raise ValueError("Input file must contain a 'company_id' column")
        
        # If company_name is not in columns, add a default
        if 'company_name' not in companies_df.columns:
            companies_df['company_name'] = "Company_" + companies_df['company_id'].astype(str)
        
        # Select companies to process
        companies_to_process = companies_df.iloc[args.start:args.start + args.limit]
        print(f"Processing {len(companies_to_process)} companies starting from index {args.start}")
        
        # Initialize WebDriver for Chrome
        browser = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
        
        # Login to LinkedIn
        login_to_linkedin(browser, args.username, args.password)
        
        # Wait after login
        time.sleep(5)
        
        # Process each company
        for index, row in companies_to_process.iterrows():
            company_id = str(row['company_id'])
            company_name = row['company_name']
            
            print(f"\nProcessing company {index - args.start + 1}/{len(companies_to_process)}: {company_name} (ID: {company_id})")
            
            try:
                # Create company-specific output directory
                company_output_dir = create_directory(os.path.join(batch_output_dir, f"{company_name}_{company_id}"))
                
                # Take a screenshot of the company page
                browser.get(f"https://www.linkedin.com/company/{company_id}")
                time.sleep(3)
                capture_screenshot(browser, f"{company_name}_{company_id}_profile.png")
                
                # Scrape all-time ads
                all_time_data = scrape_ad_library(browser, company_id, company_name, "all-time")
                
                # Scrape last 30 days ads
                last_30_days_data = scrape_ad_library(browser, company_id, company_name, "last-30-days")
                
                # Combine the data
                combined_data = {
                    "company_name": company_name,
                    "company_id": company_id,
                    "scrape_date": datetime.now().strftime("%Y-%m-%d"),
                    "all_time": all_time_data,
                    "last_30_days": last_30_days_data
                }
                
                # Save raw data to JSON
                json_file = os.path.join(company_output_dir, f"{company_name}_ads_data.json")
                with open(json_file, "w", encoding="utf-8") as f:
                    json.dump(combined_data, f, indent=4)
                print(f"Raw data saved to {json_file}")
                
                # Create summary entry
                summary_entry = {
                    "company_id": company_id,
                    "company_name": company_name,
                    "all_time_ads_count": all_time_data["total_ads"],
                    "last_30_days_ads_count": last_30_days_data["total_ads"],
                    "ads_running": all_time_data["ads_running"] or last_30_days_data["ads_running"],
                    "scrape_date": datetime.now().strftime("%Y-%m-%d")
                }
                results_summary.append(summary_entry)
                
                # Save progress after each company
                summary_df = pd.DataFrame(results_summary)
                summary_df.to_csv(os.path.join(batch_output_dir, "batch_results_summary.csv"), index=False)
                
                print(f"Completed processing for {company_name}")
                
                # Pause between companies
                time.sleep(5)
                
            except Exception as e:
                print(f"Error processing company {company_name} (ID: {company_id}): {e}")
                
                # Add to summary with error
                error_entry = {
                    "company_id": company_id,
                    "company_name": company_name,
                    "all_time_ads_count": 0,
                    "last_30_days_ads_count": 0,
                    "ads_running": False,
                    "scrape_date": datetime.now().strftime("%Y-%m-%d"),
                    "error": str(e)
                }
                results_summary.append(error_entry)
                
                # Save progress even after error
                summary_df = pd.DataFrame(results_summary)
                summary_df.to_csv(os.path.join(batch_output_dir, "batch_results_summary.csv"), index=False)
        
        print(f"\nBatch processing complete. Processed {len(results_summary)} companies.")
        print(f"Summary saved to {os.path.join(batch_output_dir, 'batch_results_summary.csv')}")
        
    except Exception as e:
        print(f"Error in batch processing: {e}")
    finally:
        # Close the browser
        if 'browser' in locals():
            browser.quit()
            print("Browser closed")

if __name__ == "__main__":
    main() 
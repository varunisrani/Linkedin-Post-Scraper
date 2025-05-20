#!/usr/bin/env python3
# Import necessary libraries
import os
import time
import pandas as pd
import argparse
import logging
import datetime
import multiprocessing
from multiprocessing import Pool, cpu_count
import subprocess
import json
from urllib.parse import urlparse
from functools import partial
import random
import queue
import threading
from collections import deque

# Import necessary selenium components directly
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, WebDriverException

# Import the ad scraper functions from the original script
from linkedin_ad_scraper import create_directory, scrape_ad_library, capture_screenshot

# Setup logging
def setup_logging(log_level=logging.INFO):
    """Setup logging configuration with file and console handlers"""
    # Create logs directory
    logs_dir = create_directory("logs")
    
    # Create a timestamp for the log filename
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(logs_dir, f"parallel_scraper_{timestamp}.log")
    
    # Configure logging
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    
    logging.info(f"Logging initialized. Log file: {log_file}")
    return log_file

# List of user agents to rotate through to help avoid rate limiting
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 11.5; rv:90.0) Gecko/20100101 Firefox/90.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36 Edg/92.0.902.55",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36"
]

def get_random_user_agent():
    """Return a random user agent from the list"""
    return random.choice(USER_AGENTS)

def with_retry(func, max_retries=3, base_delay=10):
    """
    Decorator to retry a function with exponential backoff when rate limited
    
    Args:
        func: The function to retry
        max_retries: Maximum number of retries
        base_delay: Initial delay in seconds (will be increased exponentially)
    """
    def wrapper(*args, **kwargs):
        retries = 0
        while retries <= max_retries:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if "429" in str(e) or "rate limit" in str(e).lower() or "too many requests" in str(e).lower():
                    if retries == max_retries:
                        logging.error(f"Rate limit exceeded after {max_retries} retries. Giving up.")
                        raise
                    
                    # Calculate backoff delay: base_delay * 2^retries + random jitter
                    delay = base_delay * (2 ** retries) + random.uniform(1, 5)
                    logging.warning(f"Rate limit detected (HTTP 429). Retrying in {delay:.2f} seconds...")
                    time.sleep(delay)
                    retries += 1
                else:
                    # Not a rate limit error, re-raise
                    raise
    return wrapper

def handle_rate_limiting(driver, company_name):
    """
    Handle rate limiting by clearing cookies and pausing
    
    Args:
        driver: The Selenium WebDriver instance
        company_name: Name of the company being scraped (for logging)
    """
    thread_name = threading.current_thread().name
    logging.warning(f"Thread {thread_name}: Rate limiting detected for {company_name}. Taking recovery actions...")
    
    try:
        # Clear cookies
        driver.delete_all_cookies()
        logging.info(f"Thread {thread_name}: Cleared browser cookies")
        
        # Wait longer to help avoid rate limiting
        delay = random.uniform(30, 60)
        logging.info(f"Thread {thread_name}: Waiting {delay:.2f}s before retrying...")
        time.sleep(delay)
        
        return True
    except Exception as e:
        logging.error(f"Thread {thread_name}: Error while handling rate limiting: {e}")
        return False

def process_company_in_tab(browser, tab_index, company_data):
    """Process a single company in a specific browser tab"""
    try:
        company_name = company_data['company_name']
        company_id = company_data['company_id']
        
        thread_name = threading.current_thread().name
        logging.info(f"Thread {thread_name}: Starting to scrape ads for {company_name} (ID: {company_id}) in tab {tab_index}")
        
        # Switch to the specified tab
        browser.switch_to.window(browser.window_handles[tab_index])
        
        # Create output directories for this company
        safe_company_name = company_name.replace(' ', '_').replace('/', '_').replace('\\', '_')
        output_dir = create_directory(f"{safe_company_name}_output")
        html_dir = create_directory(os.path.join(output_dir, "html"))
        
        # Scrape all-time ads
        logging.info(f"Thread {thread_name}: Scraping all-time ads for company ID: {company_id} in tab {tab_index}")
        all_time_data = scrape_ad_library(browser, company_id, safe_company_name, "all-time")
        
        # Scrape last 30 days ads
        logging.info(f"Thread {thread_name}: Scraping last 30 days ads for company ID: {company_id} in tab {tab_index}")
        last_30_days_data = scrape_ad_library(browser, company_id, safe_company_name, "last-30-days")
        
        # Combine the data
        combined_data = {
            "company_name": company_name,
            "company_id": company_id,
            "scrape_date": datetime.datetime.now().strftime("%Y-%m-%d"),
            "all_time": all_time_data,
            "last_30_days": last_30_days_data
        }
        
        # Create DataFrames for CSV export
        if all_time_data["ads"]:
            all_time_df = pd.DataFrame(all_time_data["ads"])
            csv_file = os.path.join(output_dir, f"{company_name}_all_time_ads.csv")
            all_time_df.to_csv(csv_file, index=False)
            logging.info(f"Thread {thread_name}: All-time ads data saved to {csv_file}")
        
        if last_30_days_data["ads"]:
            last_30_days_df = pd.DataFrame(last_30_days_data["ads"])
            csv_file = os.path.join(output_dir, f"{company_name}_last_30_days_ads.csv")
            last_30_days_df.to_csv(csv_file, index=False)
            logging.info(f"Thread {thread_name}: Last 30 days ads data saved to {csv_file}")
        
        # Generate a text report
        report_file = os.path.join(output_dir, f"{company_name}_ads_report.txt")
        with open(report_file, "w", encoding="utf-8") as f:
            f.write(f"LinkedIn Ads Report for {company_name}\n")
            f.write("=" * 80 + "\n\n")
            
            # Summary
            f.write("SUMMARY\n")
            f.write("-" * 80 + "\n")
            f.write(f"Company: {company_name}\n")
            f.write(f"Company ID: {company_id}\n")
            f.write(f"Report Date: {combined_data['scrape_date']}\n\n")
            
            # All-time ads
            f.write("ALL TIME ADS\n")
            f.write("-" * 80 + "\n")
            f.write(f"Are ads currently running? {'Yes' if all_time_data['ads_running'] else 'No'}\n")
            f.write(f"Total number of ads: {all_time_data['total_ads']}\n\n")
            
            # Last 30 days ads
            f.write("LAST 30 DAYS ADS\n")
            f.write("-" * 80 + "\n")
            f.write(f"Are ads currently running? {'Yes' if last_30_days_data['ads_running'] else 'No'}\n")
            f.write(f"Total number of ads: {last_30_days_data['total_ads']}\n\n")
            
            # Add detailed ad information if available
            if all_time_data["total_ads"] > 0 or last_30_days_data["total_ads"] > 0:
                f.write("DETAILED AD INFORMATION\n")
                # Additional report details omitted for brevity
        
        logging.info(f"Thread {thread_name}: Detailed report saved to {report_file}")
        
        # Save raw data to JSON
        json_file = os.path.join(output_dir, f"{company_name}_ads_data.json")
        with open(json_file, "w", encoding="utf-8") as f:
            json.dump(combined_data, f, indent=4)
        logging.info(f"Thread {thread_name}: Raw data saved to {json_file}")
        
        logging.info(f"Thread {thread_name}: Completed scraping for {company_name} in tab {tab_index}")
        return {
            "company_name": company_name,
            "company_id": company_id,
            "tab_index": tab_index,
            "status": "success",
            "all_time_ads": all_time_data["total_ads"],
            "last_30_days_ads": last_30_days_data["total_ads"]
        }
    except Exception as e:
        logging.error(f"Thread {thread_name}: Error processing {company_data['company_name']} in tab {tab_index}: {e}")
        return {
            "company_name": company_data['company_name'],
            "company_id": company_data['company_id'],
            "tab_index": tab_index,
            "status": "error",
            "error": str(e)
        }

class TabWorker(threading.Thread):
    """Worker thread that processes companies in a specific browser tab"""
    
    def __init__(self, browser, tab_index, task_queue, result_queue, stop_event):
        super().__init__(name=f"TabWorker-{tab_index}")
        self.browser = browser
        self.tab_index = tab_index
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.stop_event = stop_event
    
    def run(self):
        logging.info(f"Thread {self.name}: Starting worker for tab {self.tab_index}")
        
        while not self.stop_event.is_set():
            try:
                # Try to get a task with a timeout
                try:
                    company_data = self.task_queue.get(timeout=1)
                except queue.Empty:
                    # No more tasks, exit
                    logging.info(f"Thread {self.name}: No more companies to process, exiting")
                    break
                
                # Process the company in this tab
                result = process_company_in_tab(self.browser, self.tab_index, company_data)
                
                # Add the result to the result queue
                self.result_queue.put(result)
                
                # Mark the task as done
                self.task_queue.task_done()
                
                # Add a delay before processing the next company
                if not self.task_queue.empty() and not self.stop_event.is_set():
                    delay = 30  # 30-second delay between companies as requested
                    logging.info(f"Thread {self.name}: Waiting {delay} seconds before processing the next company...")
                    time.sleep(delay)
            
            except Exception as e:
                logging.error(f"Thread {self.name}: Error in worker thread: {e}")
                # Don't break the loop, try to process the next company
        
        logging.info(f"Thread {self.name}: Worker for tab {self.tab_index} stopped")

def run_multitab_scraper(companies_df, num_tabs=3, headless=True):
    """
    Run a multi-tab scraper that uses a single browser instance with multiple tabs
    
    Args:
        companies_df: DataFrame containing company data
        num_tabs: Number of tabs to use for parallel scraping
        headless: Whether to run in headless mode
    """
    logging.info(f"Running multi-tab scraper with {num_tabs} tabs")
    
    # Initialize Chrome options
    chrome_options = Options()
    if headless:
        chrome_options.add_argument("--headless")
        logging.info("Running in headless mode")
    else:
        logging.info("Running in visible mode")
    
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    
    # Add random user agent
    user_agent = get_random_user_agent()
    chrome_options.add_argument(f"--user-agent={user_agent}")
    logging.info(f"Using user agent: {user_agent}")
    
    # Initialize the browser once
    logging.info("Initializing Chrome WebDriver")
    browser = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    
    try:
        # Create additional tabs
        logging.info(f"Creating {num_tabs-1} additional tabs")
        for i in range(num_tabs - 1):
            browser.execute_script("window.open('about:blank', '_blank');")
            logging.info(f"Created tab {i+1}")
        
        # Verify we have the correct number of tabs
        if len(browser.window_handles) != num_tabs:
            logging.warning(f"Expected {num_tabs} tabs, but got {len(browser.window_handles)}")
        
        # Create queues for task distribution and result collection
        task_queue = queue.Queue()
        result_queue = queue.Queue()
        stop_event = threading.Event()
        
        # Add all companies to the task queue
        for _, row in companies_df.iterrows():
            task_queue.put(row.to_dict())
        
        # Create and start worker threads for each tab
        workers = []
        for i in range(min(num_tabs, len(browser.window_handles))):
            worker = TabWorker(browser, i, task_queue, result_queue, stop_event)
            workers.append(worker)
            worker.start()
            # Small delay to ensure tabs are initialized properly
            time.sleep(1)
        
        # Wait for all tasks to be completed
        task_queue.join()
        
        # Signal all workers to stop
        stop_event.set()
        
        # Wait for all workers to finish
        for worker in workers:
            worker.join()
        
        # Collect results
        results = []
        while not result_queue.empty():
            results.append(result_queue.get())
        
        # Create a summary of results
        success_count = sum(1 for r in results if r['status'] == 'success')
        error_count = sum(1 for r in results if r['status'] == 'error')
        
        logging.info("\nSCRAPING SUMMARY (MULTI-TAB)")
        logging.info(f"Total companies processed: {len(results)}")
        logging.info(f"Successfully scraped: {success_count}")
        logging.info(f"Failed to scrape: {error_count}")
        if len(results) > 0:
            logging.info(f"Success rate: {success_count/len(results)*100:.2f}%")
        
        print("\nSCRAPING SUMMARY (MULTI-TAB)")
        print(f"Total companies processed: {len(results)}")
        print(f"Successfully scraped: {success_count}")
        print(f"Failed to scrape: {error_count}")
        if len(results) > 0:
            print(f"Success rate: {success_count/len(results)*100:.2f}%")
        
        # Save results to JSON for reference
        results_dir = create_directory("results")
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        results_file = os.path.join(results_dir, f"scraping_results_multitab_{timestamp}.json")
        
        with open(results_file, 'w') as f:
            json.dump(results, f, indent=4)
        
        logging.info(f"Detailed results saved to {results_file}")
        print(f"Detailed results saved to {results_file}")
        
        return results
    
    finally:
        # Close the browser at the end of all processing
        logging.info("Closing browser after processing all companies")
        browser.quit()
        logging.info("Browser closed")

def run_sequential_scraper(companies_df, headless=True):
    """
    Run a sequential scraper that reuses the same browser instance for all companies
    """
    logging.info("Running sequential scraper with browser reuse")
    
    # Initialize Chrome options
    chrome_options = Options()
    if headless:
        chrome_options.add_argument("--headless")
        logging.info("Running in headless mode")
    else:
        logging.info("Running in visible mode")
    
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    
    # Add random user agent
    user_agent = get_random_user_agent()
    chrome_options.add_argument(f"--user-agent={user_agent}")
    logging.info(f"Using user agent: {user_agent}")
    
    # Initialize the browser once
    logging.info("Initializing Chrome WebDriver")
    browser = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    
    try:
        # Process each company sequentially with the same browser instance
        results = []
        total_companies = len(companies_df)
        
        for index, row in companies_df.iterrows():
            company_data = row.to_dict()
            logging.info(f"Processing company {index+1}/{total_companies}: {company_data['company_name']}")
            
            # Process the company
            result = process_company_in_tab(browser, 0, company_data)
            results.append(result)
            
            # Add a 30-second delay between companies as requested
            if index < total_companies - 1:  # Skip delay after the last company
                delay = 30  # Fixed 30-second delay as requested
                logging.info(f"Waiting {delay} seconds before processing the next company...")
                time.sleep(delay)
        
        # Create a summary of results
        success_count = sum(1 for r in results if r['status'] == 'success')
        error_count = sum(1 for r in results if r['status'] == 'error')
        
        logging.info("\nSCRAPING SUMMARY (SEQUENTIAL WITH BROWSER REUSE)")
        logging.info(f"Total companies processed: {len(results)}")
        logging.info(f"Successfully scraped: {success_count}")
        logging.info(f"Failed to scrape: {error_count}")
        if len(results) > 0:
            logging.info(f"Success rate: {success_count/len(results)*100:.2f}%")
        
        print("\nSCRAPING SUMMARY (SEQUENTIAL WITH BROWSER REUSE)")
        print(f"Total companies processed: {len(results)}")
        print(f"Successfully scraped: {success_count}")
        print(f"Failed to scrape: {error_count}")
        if len(results) > 0:
            print(f"Success rate: {success_count/len(results)*100:.2f}%")
        
        # Save results to JSON for reference
        results_dir = create_directory("results")
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        results_file = os.path.join(results_dir, f"scraping_results_sequential_reuse_{timestamp}.json")
        
        with open(results_file, 'w') as f:
            json.dump(results, f, indent=4)
        
        logging.info(f"Detailed results saved to {results_file}")
        print(f"Detailed results saved to {results_file}")
        
        return results
    
    finally:
        # Close the browser at the end of all processing
        logging.info("Closing browser after processing all companies")
        browser.quit()
        logging.info("Browser closed")

def main():
    """Main function to parse arguments and run the scrapers"""
    parser = argparse.ArgumentParser(description="LinkedIn Ad Library Parallel Scraper")
    
    # Input arguments
    parser.add_argument("--csv", default="output/company_ids.csv", 
                        help="CSV file with company IDs (default: output/company_ids.csv)")
    parser.add_argument("--tabs", type=int, default=3,
                        help="Number of browser tabs to use for parallel scraping (default: 3)")
    parser.add_argument("--headless", action="store_true", 
                        help="Run browsers in headless mode")
    parser.add_argument("--debug", action="store_true", 
                        help="Enable debug logging")
    parser.add_argument("--max-per-run", type=int, default=None,
                        help="Maximum number of companies to process in a single run (to avoid rate limits)")
    parser.add_argument("--mode", choices=["multitab", "sequential"], default="multitab",
                        help="Scraping mode: multitab (parallel with tabs) or sequential (default: multitab)")
    
    args = parser.parse_args()
    
    # Setup logging
    log_level = logging.DEBUG if args.debug else logging.INFO
    log_file = setup_logging(log_level)
    
    # Log the configuration
    logging.info("Starting LinkedIn Ad Library Scraper")
    logging.info(f"CSV file: {args.csv}")
    logging.info(f"Mode: {args.mode}")
    if args.mode == "multitab":
        logging.info(f"Number of tabs: {args.tabs}")
    logging.info(f"Headless mode: {args.headless}")
    
    # Read the companies CSV
    companies_df = pd.read_csv(args.csv)
    logging.info(f"Found {len(companies_df)} companies in CSV")
    
    # Ensure company_id is treated as string
    companies_df['company_id'] = companies_df['company_id'].astype(str)
    
    # Limit the number of companies if max_per_run is specified
    if args.max_per_run and args.max_per_run < len(companies_df):
        logging.info(f"Limiting to {args.max_per_run} companies per run")
        companies_df = companies_df.head(args.max_per_run)
    
    # Run the scrapers
    try:
        if args.mode == "multitab":
            # Use multi-tab mode for parallel processing
            run_multitab_scraper(
                companies_df,
                num_tabs=args.tabs,
                headless=args.headless
            )
        else:
            # Use sequential mode with browser reuse
            run_sequential_scraper(
                companies_df,
                headless=args.headless
            )
    except Exception as e:
        logging.error(f"Error running scrapers: {e}")
        print(f"Error: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main()) 
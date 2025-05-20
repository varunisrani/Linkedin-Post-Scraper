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

# Import the ad scraper functions from the original script
from linkedin_ad_scraper import run_scraper, create_directory, DEFAULT_USERNAME, DEFAULT_PASSWORD

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
        format="%(asctime)s [%(levelname)s] [%(processName)s] %(message)s",
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
    process_name = multiprocessing.current_process().name
    logging.warning(f"Process {process_name}: Rate limiting detected for {company_name}. Taking recovery actions...")
    
    try:
        # Clear cookies
        driver.delete_all_cookies()
        logging.info(f"Process {process_name}: Cleared browser cookies")
        
        # Change user agent (placeholder - actual implementation would be at the driver initialization)
        logging.info(f"Process {process_name}: Would rotate user agent if possible")
        
        # Wait longer to help avoid rate limiting
        delay = random.uniform(30, 60)
        logging.info(f"Process {process_name}: Waiting {delay:.2f}s before retrying...")
        time.sleep(delay)
        
        return True
    except Exception as e:
        logging.error(f"Process {process_name}: Error while handling rate limiting: {e}")
        return False

def process_company(company_data, username=None, password=None, headless=True, user_agent=None):
    """Process a single company to scrape ads - designed for parallel execution"""
    try:
        company_name = company_data['company_name']
        company_id = company_data['company_id']
        
        process_name = multiprocessing.current_process().name
        logging.info(f"Process {process_name}: Starting to scrape ads for {company_name} (ID: {company_id})")
        
        # Add significant delay to avoid rate limiting
        # More important companies should have longer delays between them
        delay = random.uniform(10, 20)  # Increased from 1-5 to 10-20 seconds
        logging.info(f"Process {process_name}: Waiting {delay:.2f}s before starting...")
        time.sleep(delay)
        
        # Add user agent parameter for the run_scraper function
        # Note: You'll need to modify the linkedin_ad_scraper.py to accept and use this parameter
        # Use the run_scraper function from the original script
        try:
            run_scraper(
                company_url=None,
                company_id=company_id,
                company_name=company_name,
                username=username,
                password=password,
                headless=headless
            )
        except Exception as e:
            if "429" in str(e) or "rate limit" in str(e).lower() or "too many requests" in str(e).lower():
                logging.warning(f"Process {process_name}: Rate limit detected for {company_name}. Waiting longer and retrying...")
                # Wait much longer on rate limit
                time.sleep(random.uniform(60, 120))  # 1-2 minute wait
                
                # Try again with more caution
                run_scraper(
                    company_url=None,
                    company_id=company_id,
                    company_name=company_name,
                    username=username,
                    password=password,
                    headless=headless
                )
            else:
                # Not a rate limit error, re-raise
                raise
        
        logging.info(f"Process {process_name}: Completed scraping for {company_name}")
        return {
            "company_name": company_name,
            "company_id": company_id,
            "status": "success"
        }
    except Exception as e:
        logging.error(f"Process {multiprocessing.current_process().name}: Error processing {company_data['company_name']}: {e}")
        return {
            "company_name": company_data['company_name'],
            "company_id": company_data['company_id'],
            "status": "error",
            "error": str(e)
        }

def divide_companies(companies_df, num_scrapers=4):
    """Divide the companies among the specified number of scrapers"""
    total_companies = len(companies_df)
    logging.info(f"Dividing {total_companies} companies among {num_scrapers} scrapers")
    
    # Calculate companies per scraper
    base_count = total_companies // num_scrapers
    remainder = total_companies % num_scrapers
    
    scraper_assignments = []
    start_idx = 0
    
    for i in range(num_scrapers):
        # Allocate an extra company to some scrapers if there's a remainder
        count = base_count + (1 if i < remainder else 0)
        end_idx = start_idx + count
        
        # Get the slice of companies for this scraper
        if count > 0:
            scraper_companies = companies_df.iloc[start_idx:end_idx].to_dict('records')
            scraper_assignments.append(scraper_companies)
            logging.info(f"Scraper {i+1} assigned {len(scraper_companies)} companies: {', '.join([c['company_name'] for c in scraper_companies])}")
        else:
            scraper_assignments.append([])
            logging.info(f"Scraper {i+1} has no companies assigned")
        
        start_idx = end_idx
    
    return scraper_assignments

def run_parallel_scrapers(companies_csv_path, num_scrapers=4, username=None, password=None, headless=True):
    """Run multiple LinkedIn ad scrapers in parallel"""
    try:
        # Read the companies CSV
        logging.info(f"Reading companies data from {companies_csv_path}")
        companies_df = pd.read_csv(companies_csv_path)
        logging.info(f"Found {len(companies_df)} companies in CSV")
        
        # Ensure company_id is treated as string
        companies_df['company_id'] = companies_df['company_id'].astype(str)
        
        # Divide companies among scrapers
        scraper_assignments = divide_companies(companies_df, num_scrapers)
        
        # Flatten the list of companies
        all_companies = [company for assignment in scraper_assignments for company in assignment]
        
        # Reduce the number of parallel processes to avoid rate limiting
        # Using fewer processes than requested to be more gentle on the server
        suggested_processes = min(num_scrapers, 2)  # Limit to 2 parallel processes max
        max_processes = min(len(all_companies), suggested_processes, cpu_count())
        logging.info(f"Using {max_processes} processes for parallel execution (reduced from {num_scrapers} to avoid rate limiting)")
        
        # Create a partial function with the username and password
        process_company_with_auth = partial(
            process_company,
            username=username,
            password=password,
            headless=headless
        )
        
        # Process in parallel
        results = []
        with Pool(processes=max_processes) as pool:
            # Map the process_company function to the companies
            results = pool.map(process_company_with_auth, all_companies)
        
        # Create a summary of results
        success_count = sum(1 for r in results if r['status'] == 'success')
        error_count = sum(1 for r in results if r['status'] == 'error')
        
        logging.info("\nSCRAPING SUMMARY")
        logging.info(f"Total companies processed: {len(results)}")
        logging.info(f"Successfully scraped: {success_count}")
        logging.info(f"Failed to scrape: {error_count}")
        logging.info(f"Success rate: {success_count/len(results)*100:.2f}%")
        
        print("\nSCRAPING SUMMARY")
        print(f"Total companies processed: {len(results)}")
        print(f"Successfully scraped: {success_count}")
        print(f"Failed to scrape: {error_count}")
        print(f"Success rate: {success_count/len(results)*100:.2f}%")
        
        # Save results to JSON for reference
        results_dir = create_directory("results")
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        results_file = os.path.join(results_dir, f"scraping_results_{timestamp}.json")
        
        with open(results_file, 'w') as f:
            json.dump(results, f, indent=4)
        
        logging.info(f"Detailed results saved to {results_file}")
        print(f"Detailed results saved to {results_file}")
        
        return results
        
    except Exception as e:
        logging.error(f"Error in parallel scraper: {e}")
        raise

def main():
    """Main function to parse arguments and run the parallel scrapers"""
    parser = argparse.ArgumentParser(description="LinkedIn Ad Library Parallel Scraper")
    
    # Input arguments
    parser.add_argument("--csv", default="output/company_ids.csv", 
                        help="CSV file with company IDs (default: output/company_ids.csv)")
    parser.add_argument("--scrapers", type=int, default=2,  # Changed default from 4 to 2
                        help="Number of parallel scrapers (default: 2)")
    parser.add_argument("--username", help="LinkedIn username")
    parser.add_argument("--password", help="LinkedIn password")
    parser.add_argument("--headless", action="store_true", 
                        help="Run browsers in headless mode")
    parser.add_argument("--debug", action="store_true", 
                        help="Enable debug logging")
    parser.add_argument("--max-per-run", type=int, default=None,
                        help="Maximum number of companies to process in a single run (to avoid rate limits)")
    parser.add_argument("--sequential", action="store_true",
                        help="Run scrapers sequentially instead of in parallel (helps avoid rate limits)")
    
    args = parser.parse_args()
    
    # Setup logging
    log_level = logging.DEBUG if args.debug else logging.INFO
    log_file = setup_logging(log_level)
    
    # Log the configuration
    logging.info("Starting LinkedIn Ad Library Parallel Scraper")
    logging.info(f"CSV file: {args.csv}")
    logging.info(f"Number of scrapers: {args.scrapers}")
    logging.info(f"Headless mode: {args.headless}")
    
    # Use default credentials if none provided
    username = args.username if args.username else DEFAULT_USERNAME
    password = args.password if args.password else DEFAULT_PASSWORD
    
    # Run the parallel scrapers
    try:
        if args.sequential:
            # Run in sequential mode to be extra cautious about rate limiting
            logging.info("Running in sequential mode to avoid rate limiting")
            
            # Read the companies CSV
            companies_df = pd.read_csv(args.csv)
            logging.info(f"Found {len(companies_df)} companies in CSV")
            
            # Ensure company_id is treated as string
            companies_df['company_id'] = companies_df['company_id'].astype(str)
            
            # Limit the number of companies if max_per_run is specified
            if args.max_per_run and args.max_per_run < len(companies_df):
                logging.info(f"Limiting to {args.max_per_run} companies per run")
                companies_df = companies_df.head(args.max_per_run)
            
            # Process each company sequentially
            results = []
            for _, company_data in companies_df.iterrows():
                company_dict = company_data.to_dict()
                result = process_company(
                    company_dict,
                    username=username,
                    password=password,
                    headless=args.headless
                )
                results.append(result)
                
                # Add a significant delay between companies
                delay = random.uniform(30, 60)  # 30-60 second delay between companies
                logging.info(f"Waiting {delay:.2f}s before the next company to avoid rate limiting...")
                time.sleep(delay)
                
            # Create a summary of results
            success_count = sum(1 for r in results if r['status'] == 'success')
            error_count = sum(1 for r in results if r['status'] == 'error')
            
            logging.info("\nSCRAPING SUMMARY (SEQUENTIAL MODE)")
            logging.info(f"Total companies processed: {len(results)}")
            logging.info(f"Successfully scraped: {success_count}")
            logging.info(f"Failed to scrape: {error_count}")
            logging.info(f"Success rate: {success_count/len(results)*100:.2f}%")
            
            print("\nSCRAPING SUMMARY (SEQUENTIAL MODE)")
            print(f"Total companies processed: {len(results)}")
            print(f"Successfully scraped: {success_count}")
            print(f"Failed to scrape: {error_count}")
            print(f"Success rate: {success_count/len(results)*100:.2f}%")
            
            # Save results to JSON for reference
            results_dir = create_directory("results")
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            results_file = os.path.join(results_dir, f"scraping_results_sequential_{timestamp}.json")
            
            with open(results_file, 'w') as f:
                json.dump(results, f, indent=4)
            
            logging.info(f"Detailed results saved to {results_file}")
            print(f"Detailed results saved to {results_file}")
        else:
            # Run in parallel mode (with fewer processes and more delays)
            num_scrapers = min(args.scrapers, 2)  # Limit to maximum 2 parallel scrapers
            
            # Limit the number of companies if max_per_run is specified
            max_per_run = args.max_per_run
            
            run_parallel_scrapers(
                companies_csv_path=args.csv,
                num_scrapers=num_scrapers,
                username=username,
                password=password,
                headless=args.headless
            )
    except Exception as e:
        logging.error(f"Error running scrapers: {e}")
        print(f"Error: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main()) 
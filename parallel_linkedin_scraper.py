#!/usr/bin/env python
# Import necessary libraries
import pandas as pd
import argparse
import os
import subprocess
import logging
import concurrent.futures
import time
import math
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('parallel_linkedin_scraper')

# Default credentials
DEFAULT_USERNAME = "eaglesurvivor12@gmail.com"
DEFAULT_PASSWORD = "Hello@1055"

# Folder structure plan
BASE_DIR = "linkedin_scraping_output"
CHUNK_DIRS = lambda chunk_id: os.path.join(BASE_DIR, f"chunk_{chunk_id}")
COMPANY_DIR = lambda chunk_id, company_name: os.path.join(CHUNK_DIRS(chunk_id), company_name)


def create_directory(dir_path):
    """Create a directory if it doesn't exist"""
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
        logger.info(f"Created directory: {dir_path}")
    return dir_path

def read_csv(file_path):
    """Read the CSV file with company names and LinkedIn URLs"""
    try:
        df = pd.read_csv(file_path)
        logger.info(f"Loaded CSV file with columns: {', '.join(df.columns)}")
        
        # Look for possible column names for company and URL
        company_col = None
        url_col = None
        
        # Possible names for company column
        company_variations = ['company name', 'company', 'name', 'company_name']
        url_variations = ['linkedin url', 'linkedin_url', 'url', 'linkedin link', 'link', 'linkedin']
        
        for col in df.columns:
            col_lower = col.lower().strip()
            if company_col is None and any(var in col_lower for var in company_variations):
                company_col = col
            if url_col is None and any(var in col_lower for var in url_variations):
                url_col = col
            if company_col and url_col:
                break
        
        if company_col is None or url_col is None:
            raise ValueError(f"Could not find required columns in CSV. Need one column for company name and one for LinkedIn URL.\nFound columns: {', '.join(df.columns)}")
        
        # Rename columns for internal use
        df = df.rename(columns={company_col: 'Company Name', url_col: 'LinkedIn URL'})
        logger.info(f"Using '{company_col}' as Company Name and '{url_col}' as LinkedIn URL")
        logger.info(f"Loaded {len(df)} companies from {file_path}")
        return df
    except Exception as e:
        logger.error(f"Error reading CSV file: {e}")
        raise

def divide_into_chunks(total_items, num_chunks):
    """Divide the total items into specified number of chunks"""
    chunk_size = math.ceil(total_items / num_chunks)
    chunks = [list(range(i, min(i + chunk_size, total_items))) for i in range(0, total_items, chunk_size)]
    logger.info(f"Divided {total_items} companies into {num_chunks} chunks")
    return chunks

def fetch_company_id(company_name, url, username, password, chunk_id):
    """Fetch company ID using fetch_page_source.py"""
    logger.info(f"Fetching company ID for {company_name}...")
    output_dir = create_directory(COMPANY_DIR(chunk_id, company_name))
    cmd = [
        'python', 'fetch_page_source.py',
        url,
        '--wait', '15',
        '--username', username,
        '--password', password,
        '--output', os.path.join(output_dir, f"{company_name}_id.json")
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            logger.error(f"Error fetching company ID for {company_name}: {result.stderr}")
            return None
        output_file = os.path.join(output_dir, f"{company_name}_id.json")
        if os.path.exists(output_file):
            with open(output_file, 'r') as f:
                data = json.load(f)
                company_id = data.get('company_id')
                if company_id:
                    logger.info(f"Fetched company ID {company_id} for {company_name}")
                    return company_id
        logger.warning(f"No company ID found in output for {company_name}")
        return None
    except Exception as e:
        logger.error(f"Exception while fetching company ID for {company_name}: {e}")
        return None

def scrape_ads(company_name, company_id, username, password, chunk_id):
    """Scrape ads using linkedin_ad_scraper.py"""
    logger.info(f"Scraping ads for {company_name} with ID {company_id}...")
    output_dir = create_directory(COMPANY_DIR(chunk_id, company_name))
    cmd = [
        'python', 'linkedin_ad_scraper.py',
        '--id', str(company_id),
        '--name', company_name,
        '--username', username,
        '--password', password
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, cwd=output_dir)
        if result.returncode != 0:
            logger.error(f"Error scraping ads for {company_name}: {result.stderr}")
            return False
        logger.info(f"Completed ad scraping for {company_name}")
        return True
    except Exception as e:
        logger.error(f"Exception while scraping ads for {company_name}: {e}")
        return False

def process_chunk(chunk_id, company_indices, df, username, password, delay_between_companies):
    """Process a chunk of companies"""
    logger.info(f"Starting chunk {chunk_id + 1} with {len(company_indices)} companies")
    create_directory(CHUNK_DIRS(chunk_id + 1))
    for idx in company_indices:
        company_name = df.iloc[idx]['Company Name']
        url = df.iloc[idx]['LinkedIn URL']
        logger.info(f"Processing company {idx + 1}/{len(df)} in chunk {chunk_id + 1}")
        logger.info(f"Starting to process company: {company_name}")
        company_id = fetch_company_id(company_name, url, username, password, chunk_id + 1)
        if company_id:
            scrape_ads(company_name, company_id, username, password, chunk_id + 1)
        else:
            logger.warning(f"Skipping ad scraping for {company_name} due to missing company ID")
        time.sleep(delay_between_companies)  # Delay between processing companies for accuracy
    logger.info(f"Completed chunk {chunk_id + 1}")

def main():
    """Main function to run the parallel LinkedIn ad scraper"""
    parser = argparse.ArgumentParser(description="Parallel LinkedIn Ad Scraper")
    parser.add_argument("csv_file", help="Path to CSV file with company names and LinkedIn URLs")
    parser.add_argument("--username", help="LinkedIn username", default=DEFAULT_USERNAME)
    parser.add_argument("--password", help="LinkedIn password", default=DEFAULT_PASSWORD)
    parser.add_argument("--num_chunks", type=int, default=5, help="Number of chunks to divide the companies into")
    parser.add_argument("--delay", type=int, default=10, help="Delay in seconds between processing companies")

    args = parser.parse_args()

    logger.info("Starting LinkedIn Ads Scraper")

    # Create base directory
    create_directory(BASE_DIR)

    # Read CSV
    df = read_csv(args.csv_file)

    # Divide into chunks
    chunks = divide_into_chunks(len(df), args.num_chunks)

    # Process chunks in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.num_chunks) as executor:
        futures = [
            executor.submit(
                process_chunk,
                chunk_id,
                chunk_indices,
                df,
                args.username,
                args.password,
                args.delay
            )
            for chunk_id, chunk_indices in enumerate(chunks)
        ]
        concurrent.futures.wait(futures)

    logger.info("All chunks processed. LinkedIn ad scraping completed.")

if __name__ == "__main__":
    main()

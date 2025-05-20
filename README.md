# Parallel LinkedIn Ads Scraper

This script automates scraping LinkedIn company profiles and their ad libraries in parallel. It processes a CSV list of companies, dividing them into multiple chunks that run simultaneously.

## Features

- Reads company names and LinkedIn URLs from a CSV file
- Divides companies into configurable number of parallel chunks
- First extracts company ID using `fetch_page_source.py`
- Then scrapes ads using `linkedin_ad_scraper.py` with the obtained company ID
- Processes each chunk in parallel
- Adds configurable delays between operations for rate limiting
- Creates organized folder structure for outputs

## Folder Structure

```
output/
├── scrape_summary.log
├── part_1/
│   ├── Company1/
│   │   ├── Company1_company_info.json
│   │   ├── Company1_success.log
│   │   └── Company1_output/
│   │       ├── Company1_ads_data.json
│   │       ├── Company1_ads_report.txt
│   │       └── ...
│   └── Company2/
│       └── ...
├── part_2/
│   └── ...
└── ...
```

## Prerequisites

- Python 3.6+
- Required Python packages:
  - pandas
  - selenium
  - webdriver-manager
  - beautifulsoup4
  - openai (for `fetch_page_source.py`)
- Access to both script files:
  - `fetch_page_source.py`
  - `linkedin_ad_scraper.py`
- A valid LinkedIn account

## Usage

1. Prepare a CSV file with columns: `company_name` and `linkedin_url`
2. Run the script:

```bash
python parallel_linkedin_scraper.py sample_companies.csv --username your_linkedin_username --password your_linkedin_password --num_chunks 5
```

### Command Line Arguments

- `csv_file`: Path to CSV file with company_name and linkedin_url columns (required)
- `--num_chunks`: Number of parallel chunks (default: 5)
- `--username`: LinkedIn username (required)
- `--password`: LinkedIn password (required)
- `--output_dir`: Base output directory (default: output)
- `--min_delay`: Minimum delay between fetch and scrape operations (default: 5 seconds)
- `--max_delay`: Maximum delay between fetch and scrape operations (default: 15 seconds)
- `--chunk_delay_min`: Minimum delay between companies in a chunk (default: 30 seconds)
- `--chunk_delay_max`: Maximum delay between companies in a chunk (default: 60 seconds)

## Example CSV Format

```
company_name,linkedin_url
Microsoft,https://www.linkedin.com/company/microsoft/
Apple,https://www.linkedin.com/company/apple/
Google,https://www.linkedin.com/company/google/
```

## Notes

- Adjust delay parameters to avoid LinkedIn rate limiting
- For a large number of companies, consider running fewer chunks with longer delays
- The script creates detailed logs for troubleshooting 

# LinkedIn Profile and Ad Count Scraper with Concurrent Processing

This tool fetches LinkedIn profile data and company advertisement counts using a combination of Bright Data API and web scraping with Selenium. It now supports concurrent processing to significantly speed up operations.

## New Features

### Concurrent Processing
- Process multiple profiles and companies simultaneously
- Divides input URLs into smaller batches for efficient handling
- Runs multiple Selenium instances in parallel for ad count scraping
- Optimized for large datasets with 100+ profiles

### Google Sheets Integration
- Fetch input data directly from Google Sheets
- Write results back with atomic updates to prevent conflicts
- Track progress by updating specific columns

## Setup

1. Install required dependencies:
   ```
   pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib pandas selenium webdriver-manager apify-client concurrent-futures
   ```

2. Make sure you have the client secrets file for Google Sheets API:
   - The file should be named `client_secret_<your-id>.apps.googleusercontent.com.json`
   - This file contains the credentials needed to authenticate with Google's API

## Using with Concurrent Processing

Prepare a Google Sheet with at least one column named `profileUrl` containing LinkedIn profile URLs.

### Running with Concurrent Processing

```bash
python linkedin_profile_combined_scraper.py --sheet-id YOUR_SPREADSHEET_ID --batch-size 10 --max-workers 8 --apify-token YOUR_APIFY_TOKEN
```

Where:
- `YOUR_SPREADSHEET_ID` is the ID from your Google Sheet URL
- `--batch-size` controls how many profiles to process in each batch (default: 10)
- `--max-workers` limits the number of concurrent workers (default: 10)
- `YOUR_APIFY_TOKEN` is your Apify platform API token

### Concurrent Processing Parameters

- `--batch-size`: Number of profiles to process in each batch (default: 10)
- `--max-workers`: Maximum number of concurrent workers (default: 10)

### Other Optional Parameters

- `--sheet-id`: Google Sheet ID containing profile URLs (required)
- `--visible`: Show the browser while scraping
- `--wait`: Additional wait time between actions (in seconds)
- `--debug`: Enable detailed logging
- `--linkedin-username` and `--linkedin-password`: LinkedIn credentials
- `--output`: Output CSV filename (default: profile_combined_data.csv)
- `--intermediate`: Intermediate JSON file for profile data (default: profile_data.json)
- `--company-data`: Company data JSON file (default: company_data.json)

## Performance Considerations

- For best performance, balance `batch-size` and `max-workers` based on your system resources
- Using 8-10 workers is recommended for most systems; more workers may trigger rate limiting
- Each worker requires a separate browser instance, so memory usage increases with worker count
- When processing very large datasets (100+ profiles), consider using smaller batch sizes

## Output

The script outputs a CSV file (default: `profile_combined_data.csv`) containing:
- Profile name and URL
- Company details (name, URL, ID, industry, etc.)
- Number of ads (all-time and last 30 days)
- Location and follower count
- Snapshot date

Additionally, all Google Sheet columns are updated with the latest data.

## How Concurrent Processing Works

1. Profiles are divided into batches of configurable size (default: 10)
2. Each batch is processed by a separate worker in the thread pool
3. Company data is fetched in parallel using multiple Bright Data API calls
4. Ad count scraping runs multiple browser instances simultaneously
5. Google Sheet updates are performed atomically to prevent conflicts

## Troubleshooting

- If you encounter rate limiting, reduce the number of concurrent workers
- Memory issues may occur with many browser instances; reduce `max-workers`
- For LinkedIn login issues, try running with `--visible` to manually solve captchas
- If authentication fails, delete `token.json` and try again

# LinkedIn Company Data Fetcher

This script fetches company data from LinkedIn using the Bright Data API. It reads company URLs from a CSV file and saves the results in JSON format.

## Requirements

- Python 3.6+
- `requests` library

## Installation

1. Install the required dependencies:
```bash
pip install -r requirements.txt
```

## Usage

1. Prepare your input CSV file with the following format:
```csv
company_name,company_url
Company1,https://www.linkedin.com/company/company1/
Company2,https://www.linkedin.com/company/company2/
```

2. Run the script:
```bash
python linkedin_company_fetcher.py
```

The script will:
- Read company URLs from `example_companies.csv`
- Fetch data for each company using the Bright Data API
- Save the results to `company_data.json`

## Output

The results will be saved in `company_data.json` containing detailed company information including:
- Company name
- Description
- Industry
- Location
- Employee count
- And more...

# LinkedIn Profile Scraper

This tool scrapes LinkedIn profiles using the Apify platform. It reads profile URLs from a Google Sheet and saves the scraped data in both JSON and CSV formats.

## Prerequisites

1. Python 3.8 or higher
2. An Apify account and API token
3. Google Cloud project with Sheets API enabled
4. Google OAuth 2.0 client credentials

## Setup

1. Install the required packages:
```bash
pip install -r requirements.txt
```

2. Set up Google Sheets API:
   - Create a project in Google Cloud Console
   - Enable the Google Sheets API
   - Create OAuth 2.0 credentials
   - Download the client secret file and rename it to `client_secret.json`

3. Set up your Apify token:
   - Create an account at [Apify](https://apify.com)
   - Get your API token from Account Settings
   - Either set it as an environment variable:
     ```bash
     export APIFY_TOKEN=your_token_here
     ```
   - Or use it as a command-line argument when running the script

4. Prepare your Google Sheet:
   - Create a sheet with LinkedIn profile URLs in column A
   - Make sure the URLs are in the format: https://www.linkedin.com/in/username
   - Share the sheet with the email address from your Google Cloud credentials

## Usage

Run the script with the following command:

```bash
python linkedin_profile_combined_scraper.py --sheet-id YOUR_SHEET_ID [options]
```

Required arguments:
- `--sheet-id`: The ID of your Google Sheet (found in the URL)

Optional arguments:
- `--output`: Output CSV file (default: profile_combined_data.csv)
- `--intermediate`: Intermediate JSON file (default: profile_data.json)
- `--debug`: Enable debug logging
- `--apify-token`: Apify API token (if not set in environment)

## Output

The script generates two files:
1. A JSON file with the raw data from Apify
2. A CSV file with the processed data

## Error Handling

- The script includes comprehensive logging
- Logs are written to both console and `linkedin_scraper.log`
- Failed scrapes are logged with error messages

## Rate Limits

- Be aware of Apify's rate limits and pricing
- The script includes built-in handling of rate limits

## Security Notes

- Never commit your API tokens or credentials
- Use environment variables for sensitive data
- Keep your `client_secret.json` and `token.json` files secure 
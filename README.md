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

# LinkedIn Profile and Ad Count Scraper

This tool fetches LinkedIn profile data and company advertisement counts using a combination of Bright Data API and web scraping with Selenium.

## New Feature: Google Sheets Integration

The script now supports fetching input data directly from Google Sheets instead of local CSV files.

## Setup

1. Install required dependencies:
   ```
   pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib pandas selenium webdriver-manager
   ```

2. Make sure you have the client secrets file for Google Sheets API:
   - The file should be named `client_secret_793132825043-cfe1h8m6bhtlov870gn4rct8d5tkpeml.apps.googleusercontent.com.json`
   - This file contains the credentials needed to authenticate with Google's API

## Using Google Sheets as Input

Prepare a Google Sheet with at least one column named `profileUrl` containing LinkedIn profile URLs.

### Running with Google Sheets

```bash
python linkedin_profile_combined_scraper.py --sheet-id YOUR_SPREADSHEET_ID --visible
```

Where:
- `YOUR_SPREADSHEET_ID` is the ID from your Google Sheet URL (the long string in the middle of the URL)
- Example: For `https://docs.google.com/spreadsheets/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit`, the ID is `1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms`

### Optional Parameters

- `--sheet-range`: Specify the sheet and range (default: "Sheet1!A:Z")
- `--visible`: Show the browser while scraping
- `--wait`: Additional wait time between actions (in seconds)
- `--skip-profile-fetch`: Skip fetching profile data and use cached JSON
- `--skip-ad-scrape`: Skip scraping ad counts
- `--debug`: Enable detailed logging
- `--username` and `--password`: LinkedIn credentials
- `--output`: Output CSV filename (default: profile_combined_data.csv)
- `--intermediate`: Intermediate JSON file for profile data (default: profile_data.json)

## Authentication Process

When you run the script with Google Sheets integration for the first time:

1. A browser window will open asking you to log in to your Google account
2. Select the account that has access to the spreadsheet
3. Grant permissions for the app to access your Google Sheets data
4. The browser will show "The authentication flow has completed." and can be closed
5. Your credentials will be saved to `token.json` for future use

## Still Using CSV Files?

You can still use CSV files with the original command:

```bash
python linkedin_profile_combined_scraper.py --input your_csv_file.csv
```

## Output

The script outputs a CSV file (default: `profile_combined_data.csv`) containing:
- Profile name and URL
- Company details (name, URL, ID, industry, etc.)
- Number of ads (all-time and last 30 days)
- Location and follower count
- Snapshot date

## Troubleshooting

- If the script can't find the client secrets file, make sure it's in the same directory
- If authentication fails, delete `token.json` and try again
- For LinkedIn login issues, try running with `--visible` to manually solve captchas

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
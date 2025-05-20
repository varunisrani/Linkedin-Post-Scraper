# Parallel LinkedIn Ad Scraper

This script automates the process of scraping LinkedIn ads for multiple companies in parallel. It uses two existing scripts:
1. `fetch_page_source.py` - To extract company IDs from LinkedIn company URLs
2. `linkedin_ad_scraper.py` - To scrape ads using the company IDs

## Features

- Reads company names and LinkedIn URLs from a CSV file
- Divides companies into multiple parts for parallel processing
- Processes companies sequentially within each part
- Creates an organized folder structure for results
- Adds random delays between requests to avoid rate limiting
- Generates summary reports of the scraping process

## Folder Structure

The script creates the following folder structure:
```
output_YYYY-MM-DD/
├── part_1/
│   ├── Company_Name_1/
│   │   ├── company_id.json
│   │   ├── fetch_output.log
│   │   ├── scraper_output.log
│   │   └── Company_Name_1_output/
│   │       ├── html/
│   │       ├── screenshots/
│   │       ├── Company_Name_1_ads_data.json
│   │       └── Company_Name_1_ads_report.txt
│   ├── Company_Name_2/
│   │   └── ...
│   └── part_results.csv
├── part_2/
│   └── ...
└── summary_results.csv
```

## Requirements

- Python 3.6+
- Selenium
- BeautifulSoup
- Pandas
- Chrome browser and ChromeDriver

## CSV Format

The input CSV file must have the following columns:
- `company_name`: The name of the company
- `linkedin_url`: The LinkedIn URL of the company (e.g., https://www.linkedin.com/company/microsoft/)

Example:
```csv
company_name,linkedin_url
Microsoft,https://www.linkedin.com/company/microsoft/
Google,https://www.linkedin.com/company/google/
```

## Usage

```bash
python parallel_linkedin_scraper.py companies.csv --parts 5 --username your_linkedin_email --password your_linkedin_password
```

### Arguments

- `csv_file`: Path to the CSV file containing company names and LinkedIn URLs (required)
- `--parts`: Number of parts to divide companies into for parallel processing (default: 5)
- `--username`: LinkedIn username/email (optional, uses default if not provided)
- `--password`: LinkedIn password (optional, uses default if not provided)
- `--output`: Custom output directory (optional, default: output_YYYY-MM-DD)

## Notes

- Each part processes companies sequentially to avoid LinkedIn rate limiting
- Random delays are added between requests to mimic human behavior
- The script saves detailed logs for debugging purposes
- If a company ID cannot be found, the ad scraping step is skipped for that company

## Troubleshooting

If you encounter issues:

1. Check the log files in the company directories
2. Verify your LinkedIn credentials
3. Try reducing the number of parallel parts
4. Increase the delays between requests
5. Make sure you have the latest versions of `fetch_page_source.py` and `linkedin_ad_scraper.py`
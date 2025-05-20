# LinkedIn Scraper Streamlit App

This Streamlit application provides a user-friendly interface for the LinkedIn company ID fetcher and ad scraper tools. It allows you to extract LinkedIn company IDs and scrape LinkedIn ads data with a simple, intuitive UI.

## Features

- **Company ID Fetcher**: Extract LinkedIn company IDs from company profile URLs
- **Ad Scraper**: Scrape LinkedIn ads from companies using their IDs
- **Live Logging**: View real-time progress and logs directly in the interface
- **Result Visualization**: Display and download scraped data
- **Multiple Input Methods**: Upload CSVs or enter data directly in the interface

## Installation

1. Clone the repository:
   ```
   git clone https://github.com/yourusername/Linkedin-Post-Scraper.git
   cd Linkedin-Post-Scraper
   ```

2. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```

## Usage

1. Start the Streamlit app:
   ```
   streamlit run app.py
   ```

2. Access the app in your browser (typically at http://localhost:8501)

3. Follow these steps in the app:
   - **Step 1**: Use the "Company ID Fetcher" to extract LinkedIn company IDs
     - Upload a CSV file with company names and URLs, or enter them manually
     - Configure the scraper settings
     - Run the ID fetcher and wait for the results
   
   - **Step 2**: Use the "Ad Scraper" to scrape LinkedIn ads
     - Use the company IDs fetched in Step 1, or upload your own CSV
     - Configure scraper settings including mode (multitab/sequential)
     - Enter your LinkedIn credentials
     - Run the ad scraper
     - View and download the results

## Input File Formats

### Company List CSV (for ID Fetcher)
```
company_name,company_url
Microsoft,https://www.linkedin.com/company/microsoft/
Google,https://www.linkedin.com/company/google/
```

### Company IDs CSV (for Ad Scraper)
```
company_name,company_id
Microsoft,1035
Google,1441
```

## Output

The app will create the following outputs:
- Company ID CSV files in the `output` directory
- Individual company output folders with:
  - CSV files of scraped ads
  - JSON data files
  - Text reports
- Summary results in the `results` directory

## Notes

- LinkedIn credentials may be required for some operations
- The tool includes various anti-rate limiting techniques
- Respect LinkedIn's terms of service and rate limits
- This tool is for educational purposes only

## License

[Include your license information here] 
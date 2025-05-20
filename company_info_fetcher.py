import requests
import json
import time
import sys
import csv
import os
from bs4 import BeautifulSoup
from urllib.parse import urlparse

class LinkedInCompanyFetcher:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Referer': 'https://www.google.com/',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'cross-site',
            'Sec-Fetch-User': '?1',
        }
        self.output_dir = "output"
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            
    def get_company_data(self, company_url):
        """
        Attempt to fetch basic company data from a LinkedIn company URL
        without requiring login.
        
        Args:
            company_url (str): LinkedIn company URL
            
        Returns:
            dict: Company data or error information
        """
        print(f"Fetching data for: {company_url}")
        
        result = {
            "url": company_url,
            "success": False,
            "company_name": None,
            "description": None,
            "error": None,
            "response_url": None,
            "is_redirect": False
        }
        
        try:
            # First attempt with a direct request to the company URL
            response = requests.get(company_url, headers=self.headers, allow_redirects=False)
            result["response_url"] = response.url if hasattr(response, 'url') else None
            
            # Check if we got redirected to the authwall
            if response.status_code == 302 or "authwall" in response.url:
                result["is_redirect"] = True
                redirect_url = response.headers.get('Location')
                print(f"Redirected to: {redirect_url}")
                
                # Try alternative approach - use public company data from Google search results
                print("Trying alternative method...")
                self._fetch_by_alternative_method(company_url, result)
            else:
                # We got a direct response, see if we can parse it
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Try to extract company name
                company_name_tag = soup.find('h1', {'class': 'org-top-card-summary__title'})
                if company_name_tag:
                    result["company_name"] = company_name_tag.text.strip()
                
                # Try to extract description
                description_tag = soup.find('p', {'class': 'org-top-card-summary__tagline'})
                if description_tag:
                    result["description"] = description_tag.text.strip()
                
                if result["company_name"] or result["description"]:
                    result["success"] = True
                else:
                    # Try alternative approach if we couldn't extract info
                    self._fetch_by_alternative_method(company_url, result)
                    
            # Save the response to a file
            self._save_response(company_url, response.text)
            
        except Exception as e:
            result["error"] = str(e)
            print(f"Error: {e}")
        
        return result
    
    def _fetch_by_alternative_method(self, company_url, result):
        """Try to fetch public company data using alternative methods"""
        try:
            # Parse the company name from URL
            parsed_url = urlparse(company_url)
            path_parts = parsed_url.path.strip('/').split('/')
            
            if len(path_parts) >= 2 and path_parts[0] == 'company':
                company_identifier = path_parts[1]
                
                # Try to get data from a public data source
                # This is simplified - in a real scenario you would use a proper API
                search_url = f"https://www.google.com/search?q={company_identifier}+linkedin+company"
                
                # Wait a bit to avoid rate limiting
                time.sleep(2)
                
                search_response = requests.get(search_url, headers=self.headers)
                search_soup = BeautifulSoup(search_response.text, 'html.parser')
                
                # Extract the title from search results
                search_title = search_soup.find('title')
                if search_title and company_identifier in search_title.text.lower():
                    result["company_name"] = company_identifier.replace('-', ' ').title()
                    result["success"] = True
                    result["note"] = "Limited data obtained from search results"
                    
        except Exception as e:
            result["error"] = str(e) + " (in alternative method)"
            print(f"Error in alternative method: {e}")
    
    def _save_response(self, url, content):
        """Save the response content to a file"""
        domain = urlparse(url).netloc
        path = urlparse(url).path.replace('/', '_')
        filename = f"{domain}{path}_response.html"
        filepath = os.path.join(self.output_dir, filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        
        print(f"Response saved to: {filepath}")

def process_companies_from_csv(csv_file):
    """Process all companies from a CSV file"""
    fetcher = LinkedInCompanyFetcher()
    results = []
    
    with open(csv_file, 'r') as f:
        reader = csv.DictReader(f)
        companies = list(reader)
    
    print(f"Processing {len(companies)} companies from {csv_file}")
    
    for i, company in enumerate(companies):
        print(f"\n[{i+1}/{len(companies)}] Processing {company['company_name']}")
        result = fetcher.get_company_data(company['company_url'])
        result['listed_name'] = company['company_name']
        results.append(result)
        
        # Be respectful with rate limiting
        if i < len(companies) - 1:
            wait_time = 3  # seconds
            print(f"Waiting {wait_time} seconds before next request...")
            time.sleep(wait_time)
    
    # Save results to JSON
    output_file = os.path.join(fetcher.output_dir, "company_results.json")
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\nResults saved to {output_file}")
    
    # Print summary
    successful = sum(1 for r in results if r['success'])
    print(f"\nSummary: {successful}/{len(results)} companies processed successfully")
    
    return results

def main():
    if len(sys.argv) > 1:
        # If URL is provided directly
        if sys.argv[1].startswith('http'):
            company_url = sys.argv[1]
            fetcher = LinkedInCompanyFetcher()
            result = fetcher.get_company_data(company_url)
            print(json.dumps(result, indent=2))
        # If CSV file is provided
        elif sys.argv[1].endswith('.csv'):
            csv_file = sys.argv[1]
            process_companies_from_csv(csv_file)
        else:
            print("Invalid argument. Provide either a LinkedIn company URL or a CSV file.")
    else:
        # Use example_companies.csv if it exists
        if os.path.exists('example_companies.csv'):
            process_companies_from_csv('example_companies.csv')
        else:
            print("Usage:")
            print("  python company_info_fetcher.py <linkedin_company_url>")
            print("  python company_info_fetcher.py <csv_file_with_company_urls>")
            print("\nCSV file should have 'company_name' and 'company_url' columns.")

if __name__ == "__main__":
    main() 
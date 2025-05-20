# Import necessary libraries
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup as bs
import time
import pandas as pd
import re
import os
import json
from datetime import datetime

# Initialize Chrome options
chrome_options = Options()
# Set the binary location to use the existing Chrome installation
chrome_options.binary_location = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
# Additional options to improve stability
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--window-size=1920,1080")

# LinkedIn Credentials
username = "eaglesurvivor12@gmail.com"
password = "Hello@1055"

# COMPANY CONFIGURATION
# Choose which company to scrape by uncommenting the desired company

# NIKE
# company_id = "1382"
# company_name = "Nike"

# GOLDMAN SACHS - Example from screenshots
company_id = "162479"
company_name = "Apple"

# APPLE
# company_id = "162479"
# company_name = "Apple"

# MICROSOFT
# company_id = "1035"
# company_name = "Microsoft"

def login_to_linkedin(browser):
    """Login to LinkedIn with the provided credentials"""
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

def create_directory(dir_name):
    """Create a directory if it doesn't exist"""
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)
        print(f"Created directory: {dir_name}")
    return dir_name

def capture_screenshot(browser, filename):
    """Capture a screenshot of the current page"""
    try:
        # Create screenshots directory if it doesn't exist
        screenshots_dir = create_directory("screenshots")
        filepath = os.path.join(screenshots_dir, filename)
        
        browser.save_screenshot(filepath)
        print(f"Screenshot saved to {filepath}")
        return True
    except Exception as e:
        print(f"Error capturing screenshot: {e}")
        return False

def scrape_ad_library(browser, time_period="all-time"):
    """Scrape LinkedIn Ad Library for the specified company and time period"""
    print(f"Scraping {time_period} ads for company ID: {company_id}...")
    
    # Create output directories
    output_dir = create_directory(f"{company_name}_output")
    html_dir = create_directory(os.path.join(output_dir, "html"))
    
    # Construct the URL based on time period
    if time_period == "last-30-days":
        url = f"https://www.linkedin.com/ad-library/search?companyIds={company_id}&dateOption=last-30-days"
    else:  # all-time
        url = f"https://www.linkedin.com/ad-library/search?companyIds={company_id}"
    
    browser.get(url)
    
    # Wait for the page to load
    try:
        # Wait for any of several possible elements that indicate the page has loaded
        element_present = EC.presence_of_any_element_located((
            By.CSS_SELECTOR, ".ad-library-banner-container, .ad-library-card, .ad-search-results, .ad-library-entry, .artdeco-card"
        ))
        WebDriverWait(browser, 30).until(element_present)
        print("Ad Library page loaded")
    except:
        print("Ad Library page took longer than expected to load, but continuing...")
    
    # Take a screenshot of the ad library page for debugging
    capture_screenshot(browser, f"{company_name}_{time_period}_adlibrary.png")
    
    # Allow more time for dynamic content to load
    time.sleep(15)
    
    # Look for number of ads if available
    try:
        ads_count_element = browser.find_element(By.XPATH, "//span[contains(text(), 'ads match')]")
        if ads_count_element:
            ads_count_text = ads_count_element.text
            print(f"Found ad count indicator: {ads_count_text}")
    except:
        print("Could not find ad count indicator")
    
    # Scroll down to load more ads
    scroll_count = 0
    max_scrolls = 15  # Increased max scrolls to ensure we load more content
    no_change_count = 0
    
    try:
        last_height = browser.execute_script("return document.body.scrollHeight")
    except:
        last_height = 1000
    
    print("Scrolling to load more ads...")
    while scroll_count < max_scrolls:
        try:
            # Try different scroll methods
            browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            # Also try scrolling by a fixed amount to handle lazy-loading
            browser.execute_script(f"window.scrollBy(0, 800);")
            
            # Wait longer between scrolls to ensure content loads
            time.sleep(5)
            
            # Check if we've reached the bottom
            try:
                new_height = browser.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    no_change_count += 1
                    if no_change_count >= 3:
                        print("No more ads loading after several attempts, stopping scroll")
                        break
                else:
                    no_change_count = 0
                last_height = new_height
            except:
                print("Error getting scroll height, continuing...")
                
            scroll_count += 1
            print(f"Scrolled {scroll_count} times")
            
            # Take a screenshot every few scrolls to see what's loading
            if scroll_count % 5 == 0:
                capture_screenshot(browser, f"{company_name}_{time_period}_scroll_{scroll_count}.png")
        except Exception as e:
            print(f"Error during scrolling: {e}")
            break
    
    # Take a final screenshot after scrolling
    capture_screenshot(browser, f"{company_name}_{time_period}_final.png")
    
    # Parse the HTML content
    print("Extracting ad data...")
    page_source = browser.page_source
    
    # Save the HTML for debugging
    html_file = os.path.join(html_dir, f"{company_name}_{time_period}_adlibrary.html")
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(page_source)
    
    soup = bs(page_source, "html.parser")
    
    # Extract ads information
    all_ads = []
    
    # Check if ads are running now - using multiple possible messages
    ads_running = True  # Default to True unless we find explicit "no ads" messages
    no_ads_patterns = ["No active ads", "not running ads", "No ads to show", "No results found"]
    
    for pattern in no_ads_patterns:
        no_ads_elems = soup.find_all(string=re.compile(pattern, re.IGNORECASE))
        if no_ads_elems:
            print(f"Found message indicating no ads: {no_ads_elems[0]}")
            ads_running = False
            break
    
    # Try multiple selectors for ad containers - LinkedIn uses different classes in different views
    ad_containers = []
    
    # List of possible container selectors - updated based on screenshots
    container_selectors = [
        ("div", {"class": "ad-library-entry"}),
        ("div", {"class": "ad-library-card"}),
        ("div", {"class": "ad-card"}),
        ("div", {"class": lambda c: c and "ad-library" in c}),
        ("div", {"class": lambda c: c and "ad-card" in c}),
        ("div", {"data-ad-id": True}),
        ("article", {"class": lambda c: c and "ad-" in c}),
        ("div", {"class": "artdeco-card"}),  # Generic cards used in ad library
        ("div", {"role": "article"})  # Role-based selector
    ]
    
    # Try each selector and collect all matching containers
    for selector, attrs in container_selectors:
        containers = soup.find_all(selector, attrs)
        if containers:
            print(f"Found {len(containers)} ad containers with selector {selector}, {attrs}")
            ad_containers.extend(containers)
    
    # If we still didn't find anything, try a more general approach
    if not ad_containers:
        # Look for any elements that might contain ad content
        possible_ads = soup.find_all(["div", "article"], class_=lambda c: c and any(term in str(c).lower() for term in ["ad", "sponsored", "promotion", "promoted"]))
        if possible_ads:
            print(f"Found {len(possible_ads)} potential ad elements using generic search")
            ad_containers.extend(possible_ads)
    
    # If still no containers found, look for cards with promoted content as shown in screenshots
    if not ad_containers:
        # Look for any cards with "Promoted" text
        promoted_containers = []
        promoted_elements = soup.find_all(string=re.compile("Promoted", re.IGNORECASE))
        for elem in promoted_elements:
            # Find parent container
            parent = elem.parent
            for _ in range(5):  # Look up to 5 levels up for a container
                if parent and parent.name in ["div", "article", "section"]:
                    promoted_containers.append(parent)
                    break
                if parent:
                    parent = parent.parent
                else:
                    break
        
        if promoted_containers:
            print(f"Found {len(promoted_containers)} containers with 'Promoted' text")
            ad_containers.extend(promoted_containers)
    
    # Additional approach - find all artdeco-cards that look like ads (as seen in screenshots)
    # This is specific to the layout shown in the screenshots
    artdeco_cards = soup.find_all("div", class_="artdeco-card")
    if artdeco_cards:
        for card in artdeco_cards:
            # Look for indicators that this card is an ad
            if card.find(string=re.compile("Promoted", re.IGNORECASE)) or card.find("img"):
                if card not in ad_containers:
                    ad_containers.append(card)
                    print(f"Found additional ad container (artdeco-card)")
    
    print(f"Total ad containers found: {len(ad_containers)}")
    
    # Process each ad container
    for i, ad in enumerate(ad_containers, 1):
        try:
            # Try to extract ad data
            ad_info = {}
            
            # Add container index for reference
            ad_info["container_index"] = i
            
            # Try multiple ways to find the ad title
            title_candidates = [
                ad.find("h3"),
                ad.find("h2"),
                ad.find("h1"),
                ad.find(class_=lambda c: c and "title" in str(c).lower()),
                ad.find(class_=lambda c: c and "headline" in str(c).lower()),
                ad.find("span", class_=lambda c: c and "bold" in str(c).lower()),
                ad.find(string=re.compile("Power your independence", re.IGNORECASE)),  # From screenshot
                ad.find(string=re.compile("Make your move with", re.IGNORECASE)),      # From screenshot
                ad.find(string=re.compile("Engineering careers at", re.IGNORECASE)),   # From screenshot
                ad.find(string=re.compile("Businesses has equipped", re.IGNORECASE)),  # From screenshot
            ]
            
            title_text = None
            for candidate in title_candidates:
                if candidate and hasattr(candidate, 'text') and candidate.text.strip():
                    title_text = candidate.text.strip()
                    break
                elif candidate and isinstance(candidate, str) and candidate.strip():
                    title_text = candidate.strip()
                    break
            
            # If still no title found, try longer text content that might contain a title
            if not title_text:
                # Find all paragraphs and look for potential titles
                paragraphs = ad.find_all("p")
                for p in paragraphs:
                    if p.text and len(p.text.strip()) > 0 and len(p.text.strip()) < 100:  # Assume titles are shorter than 100 chars
                        title_text = p.text.strip()
                        break
            
            ad_info["title"] = title_text if title_text else "No title found"
            print(f"Found ad title: {ad_info['title'][:50]}...")
            
            # Try multiple ways to find ad text
            text_candidates = [
                ad.find("p"),
                ad.find(class_=lambda c: c and "text" in str(c).lower()),
                ad.find(class_=lambda c: c and "content" in str(c).lower()),
                ad.find(class_=lambda c: c and "description" in str(c).lower()),
            ]
            
            text_content = None
            for candidate in text_candidates:
                if candidate and candidate.text.strip():
                    text_content = candidate.text.strip()
                    break
            
            if not text_content:
                # If no specific text element found, use all text from the container excluding title
                all_text = ad.get_text(separator=" ", strip=True)
                if title_text and title_text in all_text:
                    text_content = all_text.replace(title_text, "").strip()
                else:
                    text_content = all_text
            
            ad_info["text"] = text_content if text_content else "No text found"
            
            # Try to find images - cast a wide net
            images = []
            
            # Direct img tags
            img_tags = ad.find_all("img")
            for img in img_tags:
                if img.get("src"):
                    images.append(img.get("src"))
            
            # Background images in style attributes
            elements_with_style = ad.find_all(attrs={"style": True})
            for elem in elements_with_style:
                style = elem.get("style", "")
                if "background-image" in style:
                    # Extract URL from background-image: url("...")
                    match = re.search(r'background-image:\s*url\([\'"]?(.*?)[\'"]?\)', style)
                    if match:
                        images.append(match.group(1))
            
            # Video poster images
            video_elements = ad.find_all(["video", "button"], class_=lambda c: c and any(term in str(c).lower() for term in ["video", "player", "play"]))
            for video_elem in video_elements:
                if video_elem.get("poster"):
                    images.append(video_elem.get("poster"))
                
                # Look for play buttons (from screenshots)
                play_buttons = video_elem.find_all(class_=lambda c: c and "play" in str(c).lower())
                if play_buttons:
                    ad_info["has_video"] = True
                    
                    # Look for a parent with background image
                    for parent in [video_elem.parent, video_elem.parent.parent if video_elem.parent else None]:
                        if parent and parent.get("style") and "background-image" in parent.get("style"):
                            match = re.search(r'background-image:\s*url\([\'"]?(.*?)[\'"]?\)', parent.get("style"))
                            if match:
                                images.append(match.group(1))
            
            # Look for play button images - from screenshots
            play_buttons = ad.find_all(class_=lambda c: c and "play" in str(c).lower())
            if play_buttons:
                ad_info["has_video"] = True
                for btn in play_buttons:
                    # Check if the button has a parent with a background image
                    parent = btn.parent
                    if parent and parent.get("style") and "background-image" in parent.get("style"):
                        match = re.search(r'background-image:\s*url\([\'"]?(.*?)[\'"]?\)', parent.get("style"))
                        if match:
                            images.append(match.group(1))
            
            # Data attributes sometimes contain image URLs
            elements_with_data = ad.find_all(lambda tag: any(attr.startswith('data-') for attr in tag.attrs))
            for elem in elements_with_data:
                for attr, value in elem.attrs.items():
                    if attr.startswith('data-') and ('image' in attr or 'img' in attr) and isinstance(value, str) and ('http' in value):
                        images.append(value)
            
            # Select the first image as primary, but store all
            ad_info["image_url"] = images[0] if images else "No image found"
            ad_info["all_images"] = images
            
            # Ad status - look for status indicators
            status_candidates = [
                ad.find(string=re.compile("Active", re.IGNORECASE)),
                ad.find(string=re.compile("Running", re.IGNORECASE)),
                ad.find(string=re.compile("Inactive", re.IGNORECASE)),
                ad.find(string=re.compile("Ended", re.IGNORECASE)),
            ]
            
            for candidate in status_candidates:
                if candidate:
                    if re.search(r"active|running", candidate, re.IGNORECASE):
                        ad_info["status"] = "Active"
                    else:
                        ad_info["status"] = "Inactive"
                    break
            
            if "status" not in ad_info:
                # Default to "Active" for ads shown in library (likely to be active)
                ad_info["status"] = "Active"
            
            # Find links in the ad
            links = []
            for link in ad.find_all("a", href=True):
                if link.get("href"):
                    href = link.get("href")
                    # Filter out non-relevant LinkedIn internal links
                    if "/feed/" not in href and "/notifications/" not in href:
                        links.append(href)
            
            # Look for "View details" buttons (from screenshots)
            view_details = ad.find_all(string=re.compile("View details", re.IGNORECASE))
            for view_link in view_details:
                if view_link.parent and view_link.parent.name == "a" and view_link.parent.get("href"):
                    links.append(view_link.parent.get("href"))
            
            ad_info["link"] = links[0] if links else "No link found"
            ad_info["all_links"] = links
            
            # Get advertiser info
            ad_info["advertiser"] = company_name
            
            # Look for "Promoted" text
            promoted_text = ad.find(string=re.compile("Promoted", re.IGNORECASE))
            if promoted_text:
                ad_info["is_promoted"] = True
            
            # Add time period info
            ad_info["time_period"] = time_period
            
            # Add the ad to our collection
            all_ads.append(ad_info)
            
        except Exception as e:
            print(f"Error extracting ad data for container {i}: {e}")
            continue
    
    result = {
        "ads_running": ads_running or len(all_ads) > 0,  # If we found ads, they're running regardless of messages
        "total_ads": len(all_ads),
        "time_period": time_period,
        "ads": all_ads
    }
    
    print(f"Found {len(all_ads)} ads for {time_period}")
    return result

def main():
    """Main function to run the LinkedIn Ad scraper"""
    try:
        # Create output directory
        output_dir = create_directory(f"{company_name}_output")
        
        # Initialize WebDriver for Chrome
        browser = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
        
        # Login to LinkedIn
        login_to_linkedin(browser)
        
        # Wait after login
        time.sleep(5)
        
        # Scrape all-time ads
        all_time_data = scrape_ad_library(browser, "all-time")
        
        # Scrape last 30 days ads
        last_30_days_data = scrape_ad_library(browser, "last-30-days")
        
        # Combine the data
        combined_data = {
            "company_name": company_name,
            "company_id": company_id,
            "scrape_date": datetime.now().strftime("%Y-%m-%d"),
            "all_time": all_time_data,
            "last_30_days": last_30_days_data
        }
        
        # Create DataFrames for CSV export
        if all_time_data["ads"]:
            all_time_df = pd.DataFrame(all_time_data["ads"])
            csv_file = os.path.join(output_dir, f"{company_name}_all_time_ads.csv")
            all_time_df.to_csv(csv_file, index=False)
            print(f"All-time ads data saved to {csv_file}")
        
        if last_30_days_data["ads"]:
            last_30_days_df = pd.DataFrame(last_30_days_data["ads"])
            csv_file = os.path.join(output_dir, f"{company_name}_last_30_days_ads.csv")
            last_30_days_df.to_csv(csv_file, index=False)
            print(f"Last 30 days ads data saved to {csv_file}")
        
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
            
            # Detailed ad information
            if all_time_data["total_ads"] > 0 or last_30_days_data["total_ads"] > 0:
                f.write("DETAILED AD INFORMATION\n")
                f.write("-" * 80 + "\n\n")
                
                # Write all-time ads
                if all_time_data["total_ads"] > 0:
                    f.write("ALL-TIME ADS\n")
                    f.write("-" * 80 + "\n\n")
                    
                    for i, ad in enumerate(all_time_data["ads"], 1):
                        f.write(f"Ad #{i}\n")
                        f.write(f"Title: {ad['title']}\n")
                        f.write(f"Status: {ad['status']}\n")
                        f.write(f"Time Period: {ad['time_period']}\n\n")
                        
                        # Content
                        ad_text = ad['text']
                        if len(ad_text) > 1000:
                            ad_text = ad_text[:1000] + "... (text truncated)"
                        f.write(f"Content: {ad_text}\n\n")
                        
                        # Media and links
                        f.write(f"Primary Image URL: {ad['image_url']}\n")
                        
                        if 'has_video' in ad and ad['has_video']:
                            f.write("Contains Video: Yes\n")
                        
                        if 'all_images' in ad and len(ad['all_images']) > 1:
                            f.write("\nAll Images:\n")
                            for j, img in enumerate(ad['all_images'], 1):
                                f.write(f"{j}. {img}\n")
                        
                        f.write(f"\nPrimary Link: {ad['link']}\n")
                        
                        if 'all_links' in ad and len(ad['all_links']) > 1:
                            f.write("\nAll Links:\n")
                            for j, link in enumerate(ad['all_links'], 1):
                                f.write(f"{j}. {link}\n")
                        
                        f.write("-" * 80 + "\n\n")
                
                # Write last 30 days ads
                if last_30_days_data["total_ads"] > 0:
                    f.write("LAST 30 DAYS ADS\n")
                    f.write("-" * 80 + "\n\n")
                    
                    for i, ad in enumerate(last_30_days_data["ads"], 1):
                        f.write(f"Ad #{i}\n")
                        f.write(f"Title: {ad['title']}\n")
                        f.write(f"Status: {ad['status']}\n")
                        f.write(f"Time Period: {ad['time_period']}\n\n")
                        
                        # Content
                        ad_text = ad['text']
                        if len(ad_text) > 1000:
                            ad_text = ad_text[:1000] + "... (text truncated)"
                        f.write(f"Content: {ad_text}\n\n")
                        
                        # Media and links
                        f.write(f"Primary Image URL: {ad['image_url']}\n")
                        
                        if 'has_video' in ad and ad['has_video']:
                            f.write("Contains Video: Yes\n")
                        
                        if 'all_images' in ad and len(ad['all_images']) > 1:
                            f.write("\nAll Images:\n")
                            for j, img in enumerate(ad['all_images'], 1):
                                f.write(f"{j}. {img}\n")
                        
                        f.write(f"\nPrimary Link: {ad['link']}\n")
                        
                        if 'all_links' in ad and len(ad['all_links']) > 1:
                            f.write("\nAll Links:\n")
                            for j, link in enumerate(ad['all_links'], 1):
                                f.write(f"{j}. {link}\n")
                        
                        f.write("-" * 80 + "\n\n")
            else:
                f.write("\nNO ADS FOUND\n")
                f.write("-" * 80 + "\n\n")
                f.write("No active ads were found for this company. This could be because:\n")
                f.write("1. The company is not currently running any LinkedIn ads\n")
                f.write("2. LinkedIn's Ad Library is not displaying ads for this company\n")
                f.write("3. The scraper was unable to detect ads due to LinkedIn's structure\n\n")
                
                f.write("Suggestions:\n")
                f.write("- Try manually visiting the LinkedIn Ad Library for this company\n")
                f.write(f"- URL: https://www.linkedin.com/ad-library/search?companyIds={company_id}\n")
                f.write("- Try searching for other companies (e.g., Goldman Sachs, ID: 1028) to verify the Ad Library functionality\n")
            
        print(f"Detailed report saved to {report_file}")
        
        # Save raw data to JSON
        json_file = os.path.join(output_dir, f"{company_name}_ads_data.json")
        with open(json_file, "w", encoding="utf-8") as f:
            json.dump(combined_data, f, indent=4)
        print(f"Raw data saved to {json_file}")
        
    except Exception as e:
        print(f"Error in main function: {e}")
    finally:
        # Close the browser
        if 'browser' in locals():
            browser.quit()
            print("Browser closed")

if __name__ == "__main__":
    main() 
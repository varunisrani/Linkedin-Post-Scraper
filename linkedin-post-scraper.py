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
from datetime import datetime
from dateutil.relativedelta import relativedelta

# Initialize Chrome options
chrome_options = Options()
# Set the binary location to use the existing Chrome installation
chrome_options.binary_location = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
# Additional options to improve stability
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--window-size=1920,1080")

today = datetime.today().strftime('%Y-%m-%d')

#LinkedIn Credentials
username="eaglesurvivor12@gmail.com"
password="Hello@1055"

# Set LinkedIn page URL for scraping
page = 'https://www.linkedin.com/company/nike'

# Initialize WebDriver for Chrome using the installed version
browser = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

# Open LinkedIn login page
browser.get('https://www.linkedin.com/login')

# Wait for the login page to load
WebDriverWait(browser, 10).until(EC.presence_of_element_located((By.ID, "username")))

# Enter login credentials and submit
elementID = browser.find_element(By.ID, "username")
elementID.send_keys(username)
elementID = browser.find_element(By.ID, "password")
elementID.send_keys(password)
elementID.submit()

# Wait for login to complete (you might need to adjust this based on your account's login flow)
try:
    WebDriverWait(browser, 30).until(EC.url_changes('https://www.linkedin.com/login'))
except:
    print("Login process took longer than expected, but continuing...")

# Navigate to the posts page of the company
post_page = page + '/posts'
post_page = post_page.replace('//posts','/posts')
browser.get(post_page)

# Check if we're redirected to login page (in case login didn't work)
current_url = browser.current_url
if "login" in current_url or "authenticate" in current_url:
    print("Login may have failed - current URL indicates login page")
    print("Please check your credentials and try again")
    browser.quit()
    exit(1)

# Wait for the page to load
try:
    WebDriverWait(browser, 30).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
    time.sleep(5)  # Additional safety wait to ensure page is fully rendered
except Exception as e:
    print(f"Error waiting for page to load: {e}")
    print("Will try to continue anyway...")

# Extract company name from URL
company_name = page.rstrip('/').split('/')[-1].replace('-',' ').title()
print(f"Scraping posts for {company_name}...")

# Set parameters for scrolling through the page
SCROLL_PAUSE_TIME = 2.5  # Increased pause time for better loading
MAX_SCROLLS = 20  # Limit scrolling to avoid infinite loops
scrolls = 0
no_change_count = 0

# Wait to ensure body is fully loaded
try:
    WebDriverWait(browser, 15).until(lambda x: x.execute_script("return document.readyState") == "complete")
    print("Page fully loaded. Starting to scroll...")
except Exception as e:
    print(f"Error waiting for page to be ready: {e}")
    print("Will try to continue anyway...")

# Try to get scroll height with robust error handling
try:
    last_height = browser.execute_script("return document.body.scrollHeight")
except Exception as e:
    print(f"Error getting initial scroll height: {e}")
    print("Trying alternative method...")
    try:
        # Alternative way to get height
        last_height = browser.execute_script("return Math.max(document.body.scrollHeight, document.documentElement.scrollHeight)")
    except Exception as e:
        print(f"Alternative method also failed: {e}")
        last_height = 1000  # Set a default value
        
print(f"Initial scroll height: {last_height}")

# Scroll through the page until no new content is loaded
while True:
    try:
        # Scroll down
    browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        print(f"Scrolled {scrolls+1} times")
        
        # Wait for page to load new content
    time.sleep(SCROLL_PAUSE_TIME)
        
        # Calculate new scroll height and compare with last scroll height
        try:
    new_height = browser.execute_script("return document.body.scrollHeight")
            print(f"New height: {new_height}, Previous height: {last_height}")
            
    no_change_count = no_change_count + 1 if new_height == last_height else 0
            if no_change_count >= 3:
                print("No new content loaded after multiple scrolls. Finished scrolling.")
                break
            if MAX_SCROLLS and scrolls >= MAX_SCROLLS:
                print(f"Reached maximum scrolls limit ({MAX_SCROLLS}). Stopping.")
        break
                
            last_height = new_height
            scrolls += 1
        except Exception as e:
            print(f"Error while getting scroll height: {e}")
            print("Waiting a bit longer...")
            time.sleep(5)
            try:
                # Try alternative method to get scroll height
                new_height = browser.execute_script("return Math.max(document.body.scrollHeight, document.documentElement.scrollHeight)")
                print(f"Alternative method height: {new_height}")
    last_height = new_height
    scrolls += 1
            except Exception as e:
                print(f"Still having issues with getting page height: {e}")
                no_change_count += 1
                if no_change_count >= 3:
                    print("Too many scroll errors. Breaking the scroll loop")
                    break
    except Exception as e:
        print(f"Error during scrolling operation: {e}")
        no_change_count += 1
        if no_change_count >= 3:
            print("Too many errors while scrolling. Breaking the scroll loop")
            break
        time.sleep(5)

# After scrolling is complete, take a moment to ensure everything is loaded
print("Scrolling complete. Waiting for final page load...")
time.sleep(5)

# Parse the page source with BeautifulSoup
company_page = browser.page_source
linkedin_soup = bs(company_page.encode("utf-8"), "html.parser")

# Save the parsed HTML to a file
with open(f"{company_name}_soup.txt", "w+") as t:
    t.write(linkedin_soup.prettify())

# Extract post containers from the HTML
containers = [container for container in linkedin_soup.find_all("div",{"class":"feed-shared-update-v2"}) if 'activity' in container.get('data-urn', '')]

# Helper functions for date and reaction conversions
def get_actual_date(date):
    today = datetime.today().strftime('%Y-%m-%d')
    current_year = datetime.today().strftime('%Y')
    
    def get_past_date(days=0, weeks=0, months=0, years=0):
        date_format = '%Y-%m-%d'
        dtObj = datetime.strptime(today, date_format)
        past_date = dtObj - relativedelta(days=days, weeks=weeks, months=months, years=years)
        past_date_str = past_date.strftime(date_format)
        return past_date_str

    past_date = date
    
    if 'hour' in date:
        past_date = today
    elif 'day' in date:
        date.split(" ")[0]
        past_date = get_past_date(days=int(date.split(" ")[0]))
    elif 'week' in date:
        past_date = get_past_date(weeks=int(date.split(" ")[0]))
    elif 'month' in date:
        past_date = get_past_date(months=int(date.split(" ")[0]))
    elif 'year' in date:
        past_date = get_past_date(months=int(date.split(" ")[0]))
    else:
        split_date = date.split("-")
        if len(split_date) == 2:
            past_month = split_date[0]
            past_day =  split_date[1]
            if len(past_month) < 2:
                past_month = "0"+past_month
            if len(past_day) < 2:
                past_day = "0"+past_day
            past_date = f"{current_year}-{past_month}-{past_day}"
        elif len(split_date) == 3:
            past_month = split_date[0]
            past_day =  split_date[1]
            past_year = split_date[2]
            if len(past_month) < 2:
                past_month = "0"+past_month
            if len(past_day) < 2:
                past_day = "0"+past_day
            past_date = f"{past_year}-{past_month}-{past_day}"

    return past_date



def convert_abbreviated_to_number(s):
    """Convert abbreviated numbers like 1.2K or 3.4M to actual numbers"""
    try:
        # Handle empty strings or non-numeric text like "Comment"
        s = s.strip()
        if not s or s.lower() in ['comment', 'comments', 'like', 'likes', 'share', 'shares', 'repost', 'reposts']:
            return 0
            
    if 'K' in s:
        return int(float(s.replace('K', '')) * 1000)
    elif 'M' in s:
        return int(float(s.replace('M', '')) * 1000000)
    else:
        return int(s)
    except Exception as e:
        print(f"Error converting '{s}' to number: {e}")
        return 0



# Define a data structure to hold all the post information
posts_data = []


# Function to extract text from a container
def get_text(container, selector, attributes):
    try:
        element = container.find(selector, attributes)
        if element:
            return element.text.strip()
    except Exception as e:
        print(f"Error extracting text from {selector} with {attributes}: {e}")
    return ""


# Improved function to extract media information
def get_media_info(container):
    try:
        # Check for videos first
        video_elements = [
            ("div", {"class": "update-components-video"}, "Video"),
                  ("div", {"class": "update-components-linkedin-video"}, "Video"),
            ("div", {"class": "feed-shared-external-video__meta"}, "Youtube Video"),
            ("video", {}, "Video")
        ]
        
        for selector, attrs, media_type in video_elements:
            elements = container.find_all(selector, attrs)
            for element in elements:
                # Try to get video URL
                video_url = None
                # Look for embedded URL
                data_sources = element.find_all(attrs={"data-sources": True})
                if data_sources:
                    for src in data_sources:
                        if src.get("data-sources"):
                            video_url = "Video embedded in LinkedIn"
                            break
                
                # Look for source tag or src attribute
                sources = element.find_all("source", src=True)
                if sources:
                    for src in sources:
                        if src.get("src"):
                            video_url = src.get("src")
                            break
                
                # Check for linked video
                links = element.find_all("a", href=True)
                if links:
                    for link in links:
                        href = link.get("href", "")
                        if href and ("youtube" in href or "vimeo" in href or "video" in href):
                            video_url = href
                            break

                if video_url:
                    return video_url, media_type
        
        # Check for images
        image_elements = [
                  ("div", {"class": "update-components-image"}, "Image"),
            ("img", {}, "Image")
        ]
        
        for selector, attrs, media_type in image_elements:
            elements = container.find_all(selector, attrs)
            for element in elements:
                # For img tags, get src directly
                if selector == "img":
                    if element.get("src"):
                        return element.get("src"), media_type
                
                # For div containers, find img tags inside
                images = element.find_all("img", src=True)
                if images:
                    for img in images:
                        if img.get("src"):
                            return img.get("src"), media_type
        
        # Check for articles
        article_elements = [
                  ("article", {"class": "update-components-article"}, "Article"),
            ("a", {"class": "feed-shared-article__permalink"}, "Article")
        ]
        
        for selector, attrs, media_type in article_elements:
            elements = container.find_all(selector, attrs)
            for element in elements:
                links = element.find_all("a", href=True)
                if links:
                    for link in links:
                        href = link.get("href", "")
                        if href:
                            return href, media_type
                elif element.name == "a" and element.get("href"):
                    return element.get("href"), media_type
        
        # Check for shared posts
        shared_post = container.find("div", {"class": "feed-shared-mini-update-v2 feed-shared-update-v2__update-content-wrapper artdeco-card"})
        if shared_post:
            return "LinkedIn Shared Post", "Shared Post"
        
        # Check for polls
        poll = container.find("div", {"class": "feed-shared-poll ember-view"})
        if poll:
            return "LinkedIn Poll", "Poll"
        
    except Exception as e:
        print(f"Error extracting media info: {e}")
    
    return "None", "Unknown"


# Function to extract post text more comprehensively
def get_post_text(container):
    try:
        # Try different classes where post text might be found
        text_containers = [
            "feed-shared-update-v2__description-wrapper",
            "feed-shared-text relative feed-shared-update-v2__commentary",
            "feed-shared-text__text-view",
            "feed-shared-inline-show-more-text feed-shared-update-v2__description feed-shared-inline-show-more-text--expanded"
        ]
        
        for class_name in text_containers:
            elements = container.find_all(attrs={"class": class_name})
            for element in elements:
                text = element.text.strip()
                if text:
                    return text
        
        # Try to find any <p> tags with text that might contain post content
        paragraphs = container.find_all("p")
        if paragraphs:
            texts = [p.text.strip() for p in paragraphs if p.text.strip()]
            if texts:
                return " ".join(texts)
        
        # Try to look for specific span elements that might contain text
        spans = container.find_all("span", {"dir": "ltr"})
        if spans:
            texts = [span.text.strip() for span in spans if span.text.strip()]
            if texts:
                return " ".join(texts)
                
    except Exception as e:
        print(f"Error extracting post text: {e}")
    
    return ""


# Function to extract button text that may contain metrics
def get_button_metric(container, aria_label_fragment):
    """
    Extract metrics from buttons that have aria-labels containing specific fragments
    """
    try:
        # Find all buttons with the specified aria-label fragment
        buttons = container.find_all(
            lambda tag: tag.name == 'button' and 
            'aria-label' in tag.attrs and 
            aria_label_fragment in tag['aria-label'].lower()
        )
        
        # If we found any buttons, extract the text from the appropriate one
        if buttons:
            # Usually the second one (index 1) contains the actual count
            idx = 1 if len(buttons) > 1 else 0
            button_text = buttons[idx].text.strip()
            
            # Try to extract just the numeric part
            if button_text:
                if button_text.isdigit():
                    return button_text
                elif 'K' in button_text or 'M' in button_text:
                    # This is an abbreviated number like 1.2K, return as is
                    # The convert_abbreviated_to_number function will handle it
                    for part in button_text.split():
                        if 'K' in part or 'M' in part:
                            return part
                else:
                    # Try to extract a number using regex
                    numbers = re.findall(r'\d+', button_text)
                    if numbers:
                        return numbers[0]
                    else:
                        print(f"No numbers found in text: {button_text}")
            
            # If we couldn't extract a useful number, return 0
            return "0"
    except Exception as e:
        print(f"Error extracting {aria_label_fragment} metric: {e}")
    
    # Default return if extraction failed
    return "0"


# Main loop to process each container
for container in containers:
    # Extract post text using our improved function
    post_text = get_post_text(container)
    
    # Get media info
    media_link, media_type = get_media_info(container)
    
    # Get post date
    post_date = get_text(container, "div", {"class": "ml4 mt2 text-body-xsmall t-black--light"})
    post_date = get_actual_date(post_date)
    
    # Get metrics using our function
    reactions = get_button_metric(container, "reaction")
    comments = get_button_metric(container, "comment")
    shares = get_button_metric(container, "share")
    
    # If shares metric is 0, try with "repost" as sometimes it uses that instead of "share"
    if shares == "0":
        shares = get_button_metric(container, "repost")
    
    # Convert metrics to numeric values
    likes_numeric = convert_abbreviated_to_number(str(reactions))
    comments_numeric = convert_abbreviated_to_number(str(comments))
    shares_numeric = convert_abbreviated_to_number(str(shares))
    
    print(f"Post data - Text: {post_text[:50]}{'...' if len(post_text) > 50 else ''}, Media: {media_type}, Link: {media_link[:50]}{'...' if len(media_link) > 50 else ''}, Metrics: Likes={reactions}, Comments={comments}, Shares={shares}")
    
    # Add data to the posts_data list
    posts_data.append({
        "Post Text": post_text,
        "Media Type": media_type,
        "Media Link": media_link,
        "Post Date": post_date,
        "Likes": reactions,
        "Comments": comments,
        "Shares": shares,
        "Likes Numeric": likes_numeric,
        "Comments Numeric": comments_numeric,
        "Shares Numeric": shares_numeric
    })

# Convert the data into a DataFrame and perform data cleaning and sorting
try:
    df = pd.DataFrame(posts_data)
    for col in df.columns:
        try:
            if col in ["Likes Numeric", "Comments Numeric", "Shares Numeric"]:
            df[col] = df[col].astype(int)
        except ValueError as e:
            print(f"Error converting {col} to numeric: {e}")
    
    # Sort by engagement (sum of all metrics)
    df["Total Engagement"] = df["Likes Numeric"] + df["Comments Numeric"] + df["Shares Numeric"]
    df.sort_values(by="Total Engagement", inplace=True, ascending=False)
    
    # Drop the temporary column
    df = df.drop("Total Engagement", axis=1)
except Exception as e:
    print(f"Error processing data: {e}")

# Export the DataFrame to a CSV file
csv_file = f"{company_name}_posts.csv"
df.to_csv(csv_file, encoding='utf-8', index=False)
print(f"Data exported to {csv_file}")

# Also save the data to a text file
txt_file = f"{company_name}_posts.txt"
with open(txt_file, "w", encoding="utf-8") as f:
    f.write(f"LinkedIn Posts for {company_name}\n")
    f.write("=" * 80 + "\n\n")
    
    # Count posts by media type
    media_counts = {}
    for post in posts_data:
        media_type = post['Media Type']
        media_counts[media_type] = media_counts.get(media_type, 0) + 1
    
    # Write summary
    f.write("SUMMARY\n")
    f.write("-" * 80 + "\n")
    f.write(f"Total Posts: {len(posts_data)}\n")
    for media_type, count in media_counts.items():
        f.write(f"- {media_type}: {count} posts\n")
    f.write("\n\n")
    
    # Write detailed information for each post
    f.write("DETAILED POST DATA\n")
    f.write("-" * 80 + "\n\n")
    
    # Sort posts by engagement for text file
    sorted_posts = sorted(posts_data, 
                         key=lambda x: (x['Likes Numeric'] + x['Comments Numeric'] + x['Shares Numeric']), 
                         reverse=True)
    
    for i, post in enumerate(sorted_posts, 1):
        f.write(f"Post #{i} - {post['Media Type']}\n")
        f.write(f"Date: {post['Post Date']}\n")
        
        # Content (with ellipsis if too long)
        content = post['Post Text']
        if len(content) > 1000:
            content = content[:1000] + "... (text truncated)"
        f.write(f"Content: {content if content else '[No text content]'}\n\n")
        
        # Media info
        f.write(f"Media Type: {post['Media Type']}\n")
        f.write(f"Media Link: {post['Media Link']}\n\n")
        
        # Engagement metrics
        f.write(f"Engagement:\n")
        f.write(f"  - Likes: {post['Likes']}\n")
        f.write(f"  - Comments: {post['Comments']}\n")
        f.write(f"  - Shares: {post['Shares']}\n")
        f.write(f"  - Total Engagement: {post['Likes Numeric'] + post['Comments Numeric'] + post['Shares Numeric']}\n")
        
        f.write("-" * 80 + "\n\n")

print(f"Text report exported to {txt_file}")

# Close the browser
browser.quit()


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
from datetime import datetime
from dateutil.relativedelta import relativedelta

# Initialize Chrome options
chrome_options = Options()
today = datetime.today().strftime('%Y-%m-%d')

#LinkedIn Credentials
username=""
password=""

# Set LinkedIn page URL for scraping
page = 'https://www.linkedin.com/company/nike'

# Initialize WebDriver for Chrome using webdriver_manager
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

# Wait for the page to load
WebDriverWait(browser, 30).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
time.sleep(5)  # Additional safety wait to ensure page is fully rendered

# Extract company name from URL
company_name = page.rstrip('/').split('/')[-1].replace('-',' ').title()
print(company_name)

# Set parameters for scrolling through the page
SCROLL_PAUSE_TIME = 1.5
MAX_SCROLLS = False
scrolls = 0
no_change_count = 0

# Wait to ensure body is fully loaded
WebDriverWait(browser, 10).until(lambda x: x.execute_script("return document.readyState") == "complete")

# Now it's safe to get the scroll height
last_height = browser.execute_script("return document.body.scrollHeight")

# Scroll through the page until no new content is loaded
while True:
    # Scroll down
    browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    
    # Wait for page to load new content
    time.sleep(SCROLL_PAUSE_TIME)
    
    # Calculate new scroll height and compare with last scroll height
    try:
        new_height = browser.execute_script("return document.body.scrollHeight")
        no_change_count = no_change_count + 1 if new_height == last_height else 0
        if no_change_count >= 3 or (MAX_SCROLLS and scrolls >= MAX_SCROLLS):
            break
        last_height = new_height
        scrolls += 1
    except:
        print("Error while scrolling, waiting a bit longer...")
        time.sleep(3)
        try:
            new_height = browser.execute_script("return document.body.scrollHeight")
            last_height = new_height
        except:
            print("Still having issues with the page, breaking the scroll loop")
            break

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
    if 'K' in s:
        return int(float(s.replace('K', '')) * 1000)
    elif 'M' in s:
        return int(float(s.replace('M', '')) * 1000000)
    else:
        return int(s)



# Define a data structure to hold all the post information
posts_data = []


# Function to extract text from a container
def get_text(container, selector, attributes):
    try:
        element = container.find(selector, attributes)
        if element:
            return element.text.strip()
    except Exception as e:
        print(e)
    return ""


# Function to extract media information
def get_media_info(container):
    media_info = [("div", {"class": "update-components-video"}, "Video"),
                  ("div", {"class": "update-components-linkedin-video"}, "Video"),
                  ("div", {"class": "update-components-image"}, "Image"),
                  ("article", {"class": "update-components-article"}, "Article"),
                  ("div", {"class": "feed-shared-external-video__meta"}, "Youtube Video"),
                  ("div", {"class": "feed-shared-mini-update-v2 feed-shared-update-v2__update-content-wrapper artdeco-card"}, "Shared Post"),
                  ("div", {"class": "feed-shared-poll ember-view"}, "Other: Poll, Shared Post, etc")]
    
    for selector, attrs, media_type in media_info:
        element = container.find(selector, attrs)
        if element:
            link = element.find('a', href=True)
            return link['href'] if link else "None", media_type
    return "None", "Unknown"


# Main loop to process each container
for container in containers:
    post_text = get_text(container, "div", {"class": "feed-shared-update-v2__description-wrapper"})
    media_link, media_type = get_media_info(container)
    post_date = get_text(container, "div", {"class": "ml4 mt2 text-body-xsmall t-black--light"})
    post_date = get_actual_date(post_date)
    
    # Reactions (likes)
    reactions_element = container.find_all(lambda tag: tag.name == 'button' and 'aria-label' in tag.attrs and 'reaction' in tag['aria-label'].lower())
    reactions_idx = 1 if len(reactions_element) > 1 else 0
    post_reactions = reactions_element[reactions_idx].text.strip() if reactions_element and reactions_element[reactions_idx].text.strip() != '' else 0

    # Comments
    comment_element = container.find_all(lambda tag: tag.name == 'button' and 'aria-label' in tag.attrs and 'comment' in tag['aria-label'].lower())
    comment_idx = 1 if len(comment_element) > 1 else 0
    post_comments = comment_element[comment_idx].text.strip() if comment_element and comment_element[comment_idx].text.strip() != '' else 0

    # Shares
    shares_element = container.find_all(lambda tag: tag.name == 'button' and 'aria-label' in tag.attrs and 'repost' in tag['aria-label'].lower())
    shares_idx = 1 if len(shares_element) > 1 else 0
    post_shares = shares_element[shares_idx].text.strip() if shares_element and shares_element[shares_idx].text.strip() != '' else 0
    
    # Convert metrics to numeric values
    likes_numeric = convert_abbreviated_to_number(str(post_reactions)) if post_reactions else 0
    comments_numeric = convert_abbreviated_to_number(str(post_comments)) if post_comments else 0
    shares_numeric = convert_abbreviated_to_number(str(post_shares)) if post_shares else 0
    
    # Add data to the posts_data list
    posts_data.append({
        "Post Text": post_text,
        "Media Type": media_type,
        "Media Link": media_link,
        "Post Date": post_date,
        "Likes": post_reactions,
        "Comments": post_comments,
        "Shares": post_shares,
        "Likes Numeric": likes_numeric,
        "Comments Numeric": comments_numeric,
        "Shares Numeric": shares_numeric
    })

# Convert the data into a DataFrame and perform data cleaning and sorting
try:
    df = pd.DataFrame(posts_data)
    for col in df.columns:
        try:
            df[col] = df[col].astype(int)
        except ValueError:
            pass
    df.sort_values(by="Likes Numeric", inplace=True, ascending=False)
except Exception as e:
    print(f"Error processing data: {e}")

# Export the DataFrame to a CSV file
csv_file = f"{company_name}_posts.csv"
df.to_csv(csv_file, encoding='utf-8', index=False)
print(f"Data exported to {csv_file}")


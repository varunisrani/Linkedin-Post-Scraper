import streamlit as st
import pandas as pd
import os
import time
import datetime
import logging
import tempfile
import threading
import queue
import json
import sys
from pathlib import Path
import subprocess
import io
import base64
from contextlib import redirect_stdout, redirect_stderr

# Set page configuration
st.set_page_config(
    page_title="LinkedIn Scraper Tool",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Create a custom logging handler for Streamlit
class StreamlitLogHandler(logging.Handler):
    def __init__(self, placeholder):
        super().__init__()
        self.placeholder = placeholder
        self.logs = []
        
    def emit(self, record):
        log_entry = self.format(record)
        self.logs.append(log_entry)
        
        # Keep only the last 1000 log entries to prevent memory issues
        if len(self.logs) > 1000:
            self.logs = self.logs[-1000:]
            
        # Update the UI
        self.placeholder.code("\n".join(self.logs), language="bash")

# Setup logging
def setup_logging(log_placeholder):
    # Configure root logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Clear existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Create a formatter
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    
    # Create and configure the Streamlit handler
    st_handler = StreamlitLogHandler(log_placeholder)
    st_handler.setFormatter(formatter)
    logger.addHandler(st_handler)
    
    # Also log to stdout for debugging
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return st_handler

# Function to create a download link for a file
def get_download_link(file_path, text):
    with open(file_path, 'rb') as f:
        data = f.read()
    b64 = base64.b64encode(data).decode()
    return f'<a href="data:file/txt;base64,{b64}" download="{os.path.basename(file_path)}">{text}</a>'

# Function to create a directory if it doesn't exist
def create_directory(dir_name):
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)
        logging.info(f"Created directory: {dir_name}")
    return dir_name

# Function to run fetch_multiple_companies.py with the provided parameters
def run_fetch_companies(args, log_handler):
    logging.info(f"Starting company ID fetching with arguments: {args}")
    
    # Create the command
    cmd = ["python", "fetch_multiple_companies.py"]
    for key, value in args.items():
        if value is True:  # For flags
            cmd.append(f"--{key}")
        elif value is not False and value is not None:  # Skip False and None values
            cmd.append(f"--{key}")
            cmd.append(str(value))
    
    logging.info(f"Executing command: {' '.join(cmd)}")
    
    try:
        # Run the command and capture output
        process = subprocess.Popen(
            cmd, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            universal_newlines=True
        )
        
        # Process stdout and stderr while the command runs
        while process.poll() is None:
            stdout_line = process.stdout.readline()
            if stdout_line.strip():
                logging.info(stdout_line.strip())
            
            stderr_line = process.stderr.readline()
            if stderr_line.strip():
                logging.error(stderr_line.strip())
            
            # Short sleep to prevent CPU hogging
            time.sleep(0.1)
        
        # Process any remaining output
        for stdout_line in process.stdout.readlines():
            if stdout_line.strip():
                logging.info(stdout_line.strip())
        
        for stderr_line in process.stderr.readlines():
            if stderr_line.strip():
                logging.error(stderr_line.strip())
        
        # Check return code
        if process.returncode == 0:
            logging.info("Company ID fetching completed successfully")
            if os.path.exists("output/company_ids.csv"):
                df = pd.read_csv("output/company_ids.csv")
                return df, True
            else:
                logging.error("Output file not found after successful execution")
                return None, False
        else:
            logging.error(f"Company ID fetching failed with return code: {process.returncode}")
            return None, False
            
    except Exception as e:
        logging.error(f"Error executing company ID fetching: {e}")
        return None, False

# Function to run parallel_linkedin_ad_scraper.py with the provided parameters
def run_ad_scraper(args, log_handler):
    logging.info(f"Starting ad scraping with arguments: {args}")
    
    # Create the command
    cmd = ["python", "parallel_linkedin_ad_scraper.py"]
    for key, value in args.items():
        if value is True:  # For flags
            cmd.append(f"--{key}")
        elif value is not False and value is not None:  # Skip False and None values
            cmd.append(f"--{key}")
            cmd.append(str(value))
    
    logging.info(f"Executing command: {' '.join(cmd)}")
    
    try:
        # Run the command and capture output
        process = subprocess.Popen(
            cmd, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            universal_newlines=True
        )
        
        # Process stdout and stderr while the command runs
        while process.poll() is None:
            stdout_line = process.stdout.readline()
            if stdout_line.strip():
                logging.info(stdout_line.strip())
            
            stderr_line = process.stderr.readline()
            if stderr_line.strip():
                logging.error(stderr_line.strip())
            
            # Short sleep to prevent CPU hogging
            time.sleep(0.1)
        
        # Process any remaining output
        for stdout_line in process.stdout.readlines():
            if stdout_line.strip():
                logging.info(stdout_line.strip())
        
        for stderr_line in process.stderr.readlines():
            if stderr_line.strip():
                logging.error(stderr_line.strip())
        
        # Check return code
        if process.returncode == 0:
            logging.info("Ad scraping completed successfully")
            return True
        else:
            logging.error(f"Ad scraping failed with return code: {process.returncode}")
            return False
            
    except Exception as e:
        logging.error(f"Error executing ad scraping: {e}")
        return False

# Main app
def main():
    # Sidebar
    st.sidebar.title("LinkedIn Scraper Tool")
    
    # App mode selection
    app_mode = st.sidebar.radio(
        "Select Mode",
        ["Home", "Company ID Fetcher", "Ad Scraper"]
    )
    
    # Home page
    if app_mode == "Home":
        st.title("LinkedIn Scraper Tool")
        st.markdown("""
        ### Welcome to the LinkedIn Scraper Tool
        
        This application provides a user-friendly interface for scraping LinkedIn company information and advertisements.
        
        #### Available Modes:
        1. **Company ID Fetcher**: Extract LinkedIn company IDs from company profile URLs
        2. **Ad Scraper**: Scrape LinkedIn ads from companies using their IDs
        
        #### Workflow:
        1. First use the **Company ID Fetcher** to get company IDs
        2. Then use the **Ad Scraper** to scrape ads using those IDs
        
        #### Important Notes:
        - LinkedIn credentials may be required for some operations
        - Respect LinkedIn's terms of service and rate limits
        - The tool implements various anti-rate limiting techniques
        """)
        
        st.warning("⚠️ This tool is for educational purposes only. Use responsibly and respect LinkedIn's terms of service.")
    
    # Company ID Fetcher
    elif app_mode == "Company ID Fetcher":
        st.title("LinkedIn Company ID Fetcher")
        
        # Create tabs for different input methods
        input_tab, settings_tab, results_tab = st.tabs(["Input", "Settings", "Results"])
        
        with input_tab:
            input_method = st.radio(
                "Select Input Method",
                ["Upload CSV", "Enter URLs"]
            )
            
            if input_method == "Upload CSV":
                uploaded_file = st.file_uploader("Upload CSV with company_name,company_url columns", type="csv")
                if uploaded_file is not None:
                    df = pd.read_csv(uploaded_file)
                    st.write("Preview of uploaded data:")
                    st.dataframe(df.head())
                    
                    if st.button("Save CSV for Processing"):
                        # Create output directory
                        create_directory("output")
                        # Save the uploaded file
                        df.to_csv("company_list.csv", index=False)
                        st.success("CSV saved successfully!")
                        
            else:  # Enter URLs
                st.write("Enter company names and LinkedIn URLs:")
                
                # Initialize or get the list of companies
                if 'companies' not in st.session_state:
                    st.session_state.companies = [{"name": "", "url": ""}]
                
                # Display existing companies
                for i, company in enumerate(st.session_state.companies):
                    col1, col2, col3 = st.columns([3, 6, 1])
                    with col1:
                        st.session_state.companies[i]["name"] = st.text_input(
                            f"Company {i+1} Name", 
                            value=company["name"],
                            key=f"name_{i}"
                        )
                    with col2:
                        st.session_state.companies[i]["url"] = st.text_input(
                            f"Company {i+1} URL", 
                            value=company["url"],
                            key=f"url_{i}"
                        )
                    with col3:
                        if st.button("❌", key=f"del_{i}"):
                            if len(st.session_state.companies) > 1:  # Don't remove if it's the only one
                                st.session_state.companies.pop(i)
                                st.experimental_rerun()
                
                # Add new company button
                if st.button("Add Another Company"):
                    st.session_state.companies.append({"name": "", "url": ""})
                    st.experimental_rerun()
                
                if st.button("Save Companies for Processing"):
                    # Filter out empty entries
                    companies = [c for c in st.session_state.companies if c["name"] and c["url"]]
                    
                    if companies:
                        # Create output directory
                        create_directory("output")
                        # Create DataFrame and save
                        df = pd.DataFrame(companies)
                        df.columns = ["company_name", "company_url"]
                        df.to_csv("company_list.csv", index=False)
                        st.success(f"Saved {len(companies)} companies for processing!")
                    else:
                        st.error("No valid companies to save. Please enter at least one company name and URL.")
        
        with settings_tab:
            st.subheader("Scraper Settings")
            
            col1, col2 = st.columns(2)
            
            with col1:
                wait_time = st.slider("Wait Time (seconds)", 2, 20, 5, 
                                     help="Time to wait for each page to load")
                
                browser_mode = st.radio("Browser Mode", 
                                       ["Headless (No UI)", "Visible (Show Browser)"],
                                       index=0,
                                       help="Control whether the browser UI is visible during execution")
                
                debug_mode = st.checkbox("Debug Mode", value=False,
                                        help="Enable detailed debug logging")
            
            with col2:
                no_screenshot = st.checkbox("No Screenshots", value=False,
                                          help="Don't save screenshots of the pages")
                
                linkedin_username = st.text_input("LinkedIn Username (optional)", 
                                                 help="Your LinkedIn login email")
                
                linkedin_password = st.text_input("LinkedIn Password (optional)", 
                                                 type="password", 
                                                 help="Your LinkedIn login password")
            
            output_dir = st.text_input("Output Directory", value="output",
                                      help="Directory to save output files")
        
        with results_tab:
            st.subheader("Processing and Results")
            
            # Setup logging placeholder
            log_placeholder = st.empty()
            
            if st.button("Start Company ID Fetching", type="primary"):
                log_handler = setup_logging(log_placeholder)
                
                # Check if input file exists
                if not os.path.exists("company_list.csv"):
                    st.error("No input file found. Please upload or enter company data in the Input tab.")
                else:
                    # Build arguments
                    args = {
                        "csv": "company_list.csv",
                        "wait": wait_time,
                        "visible": browser_mode == "Visible (Show Browser)",
                        "no-screenshot": no_screenshot,
                        "output-dir": output_dir,
                        "debug": debug_mode
                    }
                    
                    # Add optional credentials if provided
                    if linkedin_username:
                        args["username"] = linkedin_username
                    if linkedin_password:
                        args["password"] = linkedin_password
                    
                    # Run the fetcher in a separate thread
                    with st.spinner("Fetching company IDs... Please wait."):
                        result_df, success = run_fetch_companies(args, log_handler)
                    
                    if success and result_df is not None:
                        st.success("Company ID fetching completed!")
                        
                        # Show results
                        st.subheader("Results")
                        st.dataframe(result_df)
                        
                        # Add download links
                        if os.path.exists(f"{output_dir}/company_ids.csv"):
                            csv_path = f"{output_dir}/company_ids.csv"
                            st.markdown(get_download_link(csv_path, "Download CSV"), unsafe_allow_html=True)
                            
                        # Show success rate
                        success_count = result_df['company_id'].notna().sum()
                        total_count = len(result_df)
                        if total_count > 0:
                            success_rate = (success_count / total_count) * 100
                            st.write(f"Successfully found IDs for {success_count} out of {total_count} companies ({success_rate:.2f}%)")
                    else:
                        st.error("Company ID fetching failed or no results found. Check the logs for details.")
                
                # Clean up
                try:
                    log_handler.stop()
                except:
                    pass
    
    # Ad Scraper
    elif app_mode == "Ad Scraper":
        st.title("LinkedIn Ad Scraper")
        
        # Create tabs
        input_tab, settings_tab, results_tab = st.tabs(["Input", "Settings", "Results"])
        
        with input_tab:
            input_method = st.radio(
                "Select Input Method",
                ["Use Fetched IDs", "Upload CSV", "Start from Beginning"]
            )
            
            if input_method == "Use Fetched IDs":
                # Check if we have company IDs from the previous step
                if os.path.exists("output/company_ids.csv"):
                    df = pd.read_csv("output/company_ids.csv")
                    st.write("Company IDs found from previous step:")
                    st.dataframe(df)
                    st.success("Using these company IDs for ad scraping")
                else:
                    st.warning("No company IDs found from previous step. Please run the Company ID Fetcher first or choose another input method.")
            
            elif input_method == "Upload CSV":
                uploaded_file = st.file_uploader("Upload CSV with company IDs", type="csv")
                if uploaded_file is not None:
                    df = pd.read_csv(uploaded_file)
                    st.write("Preview of uploaded data:")
                    st.dataframe(df.head())
                    
                    # Check if required columns exist
                    required_cols = ["company_name", "company_id"]
                    missing_cols = [col for col in required_cols if col not in df.columns]
                    
                    if missing_cols:
                        st.error(f"CSV is missing required columns: {', '.join(missing_cols)}. Please ensure the CSV contains at least company_name and company_id columns.")
                    else:
                        if st.button("Save CSV for Ad Scraping"):
                            # Create output directory
                            create_directory("output")
                            # Save the uploaded file
                            df.to_csv("output/company_ids.csv", index=False)
                            st.success("CSV saved successfully for ad scraping!")
            
            else:  # Start from Beginning
                st.info("This will run the complete workflow: fetching company IDs first, then scraping ads.")
                
                # Similar to the company input in the Company ID Fetcher
                st.write("Enter company names and LinkedIn URLs:")
                
                # Initialize or get the list of companies
                if 'companies_for_ads' not in st.session_state:
                    st.session_state.companies_for_ads = [{"name": "", "url": ""}]
                
                # Display existing companies
                for i, company in enumerate(st.session_state.companies_for_ads):
                    col1, col2, col3 = st.columns([3, 6, 1])
                    with col1:
                        st.session_state.companies_for_ads[i]["name"] = st.text_input(
                            f"Company {i+1} Name", 
                            value=company["name"],
                            key=f"ad_name_{i}"
                        )
                    with col2:
                        st.session_state.companies_for_ads[i]["url"] = st.text_input(
                            f"Company {i+1} URL", 
                            value=company["url"],
                            key=f"ad_url_{i}"
                        )
                    with col3:
                        if st.button("❌", key=f"ad_del_{i}"):
                            if len(st.session_state.companies_for_ads) > 1:  # Don't remove if it's the only one
                                st.session_state.companies_for_ads.pop(i)
                                st.experimental_rerun()
                
                # Add new company button
                if st.button("Add Another Company"):
                    st.session_state.companies_for_ads.append({"name": "", "url": ""})
                    st.experimental_rerun()
                
                if st.button("Save Companies for Complete Workflow"):
                    # Filter out empty entries
                    companies = [c for c in st.session_state.companies_for_ads if c["name"] and c["url"]]
                    
                    if companies:
                        # Create output directory
                        create_directory("output")
                        # Create DataFrame and save
                        df = pd.DataFrame(companies)
                        df.columns = ["company_name", "company_url"]
                        df.to_csv("company_list.csv", index=False)
                        st.success(f"Saved {len(companies)} companies for complete workflow!")
                    else:
                        st.error("No valid companies to save. Please enter at least one company name and URL.")
        
        with settings_tab:
            st.subheader("Ad Scraper Settings")
            
            col1, col2 = st.columns(2)
            
            with col1:
                scraper_mode = st.selectbox("Scraper Mode", 
                                          ["multitab", "sequential"],
                                          help="Multitab uses multiple tabs in parallel, sequential processes one at a time")
                
                num_tabs = st.slider("Number of Browser Tabs", 1, 5, 3, 
                                    help="Number of browser tabs to use for parallel scraping (only for multitab mode)")
                
                browser_mode = st.radio("Browser Mode", 
                                       ["Headless (No UI)", "Visible (Show Browser)"],
                                       index=0,
                                       help="Control whether the browser UI is visible during execution")
                
                debug_mode = st.checkbox("Debug Mode", value=False,
                                        help="Enable detailed debug logging")
            
            with col2:
                max_companies = st.number_input("Max Companies Per Run", min_value=1, value=None,
                                              help="Maximum number of companies to process (to avoid rate limits)")
                
                linkedin_username = st.text_input("LinkedIn Username (required)", 
                                                help="Your LinkedIn login email")
                
                linkedin_password = st.text_input("LinkedIn Password (required)", 
                                                type="password",
                                                help="Your LinkedIn login password")
                
                csv_path = st.text_input("CSV Path", value="output/company_ids.csv",
                                       help="Path to the CSV file with company IDs")
        
        with results_tab:
            st.subheader("Processing and Results")
            
            # Setup logging placeholder
            log_placeholder = st.empty()
            
            if st.button("Start Ad Scraping", type="primary"):
                log_handler = setup_logging(log_placeholder)
                
                # Check if input file exists
                if not os.path.exists(csv_path):
                    st.error(f"Input file not found: {csv_path}. Please make sure you have run the Company ID Fetcher or uploaded a valid CSV.")
                else:
                    # Validate LinkedIn credentials
                    if not linkedin_username or not linkedin_password:
                        st.warning("LinkedIn credentials are recommended for better results. Proceeding without them...")
                    
                    # Build arguments
                    args = {
                        "csv": csv_path,
                        "mode": scraper_mode,
                        "debug": debug_mode,
                        "headless": browser_mode == "Headless (No UI)"
                    }
                    
                    # Add optional args
                    if scraper_mode == "multitab":
                        args["tabs"] = num_tabs
                    
                    if max_companies:
                        args["max-per-run"] = int(max_companies)
                    
                    # Add credentials if provided
                    if linkedin_username:
                        args["username"] = linkedin_username
                    if linkedin_password:
                        args["password"] = linkedin_password
                    
                    # Run the ad scraper in a separate thread
                    with st.spinner("Scraping LinkedIn ads... This may take a while."):
                        success = run_ad_scraper(args, log_handler)
                    
                    if success:
                        st.success("Ad scraping completed successfully!")
                        
                        # Show results
                        st.subheader("Results")
                        
                        # Look for results directory
                        results_dir = "results"
                        if os.path.exists(results_dir):
                            # Find the most recent results file
                            result_files = [f for f in os.listdir(results_dir) if f.startswith("scraping_results_")]
                            if result_files:
                                # Sort by modification time (newest first)
                                result_files.sort(key=lambda x: os.path.getmtime(os.path.join(results_dir, x)), reverse=True)
                                latest_result = os.path.join(results_dir, result_files[0])
                                
                                # Load and display the results
                                with open(latest_result, 'r') as f:
                                    results_data = json.load(f)
                                
                                # Convert to DataFrame for display
                                results_df = pd.DataFrame(results_data)
                                st.dataframe(results_df)
                                
                                # Show download link
                                st.markdown(get_download_link(latest_result, "Download Results JSON"), unsafe_allow_html=True)
                                
                                # Show success rate
                                success_count = sum(1 for r in results_data if r.get('status') == 'success')
                                total_count = len(results_data)
                                if total_count > 0:
                                    success_rate = (success_count / total_count) * 100
                                    st.write(f"Successfully scraped ads for {success_count} out of {total_count} companies ({success_rate:.2f}%)")
                            else:
                                st.info("No result files found. Check the company output directories for scraped data.")
                        else:
                            st.info("No results directory found. Check the individual company output directories for scraped data.")
                        
                        # Look for company output directories
                        output_dirs = [d for d in os.listdir('.') if os.path.isdir(d) and d.endswith('_output')]
                        if output_dirs:
                            st.subheader("Company Output Directories")
                            for dir_name in output_dirs:
                                company_name = dir_name.replace('_output', '')
                                
                                # Check for CSV files in the directory
                                csv_files = [f for f in os.listdir(dir_name) if f.endswith('.csv')]
                                json_files = [f for f in os.listdir(dir_name) if f.endswith('.json')]
                                
                                if csv_files or json_files:
                                    with st.expander(f"{company_name} Data"):
                                        # Show CSV download links
                                        for csv_file in csv_files:
                                            file_path = os.path.join(dir_name, csv_file)
                                            st.markdown(get_download_link(file_path, f"Download {csv_file}"), unsafe_allow_html=True)
                                        
                                        # Show JSON download links
                                        for json_file in json_files:
                                            file_path = os.path.join(dir_name, json_file)
                                            st.markdown(get_download_link(file_path, f"Download {json_file}"), unsafe_allow_html=True)
                    else:
                        st.error("Ad scraping failed. Check the logs for details.")
                
                # Clean up
                try:
                    log_handler.stop()
                except:
                    pass

# Run the app
if __name__ == "__main__":
    main() 
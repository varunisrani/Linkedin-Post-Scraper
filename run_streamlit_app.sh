#!/bin/bash

# Activate virtual environment if it exists
if [ -d "venv" ]; then
    echo "Activating virtual environment..."
    source venv/bin/activate
fi

# Check if streamlit is installed
if ! command -v streamlit &> /dev/null; then
    echo "Streamlit not found, installing requirements..."
    pip install -r requirements.txt
fi

# Run the Streamlit app
echo "Starting LinkedIn Scraper Streamlit app..."
streamlit run app.py 
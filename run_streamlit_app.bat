@echo off
echo Starting LinkedIn Scraper Streamlit app...

REM Check if virtual environment exists and activate it
if exist venv\Scripts\activate.bat (
    echo Activating virtual environment...
    call venv\Scripts\activate.bat
)

REM Check if streamlit is installed
where streamlit >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo Streamlit not found, installing requirements...
    pip install -r requirements.txt
)

REM Run the Streamlit app
streamlit run app.py

pause 
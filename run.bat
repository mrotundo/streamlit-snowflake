@echo off
REM Run script for AI Chat Application on Windows
REM This script activates the virtual environment and starts the Streamlit app

echo Starting AI Chat Application...

REM Check if virtual environment exists
if not exist "venv" (
    echo Error: Virtual environment not found!
    echo Please run setup.bat first to create the virtual environment.
    pause
    exit /b 1
)

REM Check if .env file exists
if not exist ".env" (
    echo Error: .env file not found!
    echo Creating .env from template...
    copy .env.example .env
    echo Please edit .env file and add your API keys!
    pause
    exit /b 1
)

REM Activate virtual environment
echo Activating virtual environment...
call venv\Scripts\activate.bat

REM Check if Streamlit is installed
python -c "import streamlit" 2>nul
if errorlevel 1 (
    echo Dependencies not installed!
    echo Installing dependencies...
    pip install -r requirements-local.txt
)

REM Optional: Check for watchdog
python -c "import watchdog" 2>nul
if errorlevel 1 (
    echo.
    echo Tip: Install watchdog for better file watching performance
    echo Run: pip install watchdog
    echo.
)

REM Display app info
echo.
echo Starting Streamlit application...
echo The app will open in your browser automatically
echo To stop the server, press Ctrl+C
echo.

REM Run Streamlit
streamlit run app.py
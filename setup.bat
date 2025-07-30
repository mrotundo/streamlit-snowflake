@echo off
REM Setup script for AI Chat Application on Windows
REM This script creates a virtual environment and installs dependencies

echo Setting up AI Chat Application...

REM Check if Python is installed
python --version >nul 2>&1
if errorlevel 1 (
    echo Error: Python is not installed or not in PATH. Please install Python 3.8 or higher.
    pause
    exit /b 1
)

REM Get Python version
for /f "tokens=2" %%i in ('python --version 2^>^&1') do set PYTHON_VERSION=%%i
echo Found Python %PYTHON_VERSION%

REM Check if virtual environment exists
if exist "venv" (
    set /p response="Virtual environment already exists. Do you want to recreate it? (y/n): "
    if /i "%response%"=="y" (
        echo Removing existing virtual environment...
        rmdir /s /q venv
    ) else (
        echo Using existing virtual environment...
    )
)

REM Create virtual environment if it doesn't exist
if not exist "venv" (
    echo Creating virtual environment...
    python -m venv venv
)

REM Activate virtual environment
echo Activating virtual environment...
call venv\Scripts\activate.bat

REM Upgrade pip
echo Upgrading pip...
python -m pip install --upgrade pip

REM Ask user about installation type
echo.
echo Which installation type do you want?
echo 1) Local only (OpenAI support only)
echo 2) Full installation (OpenAI + Snowflake support)
echo.
set /p choice="Enter your choice (1 or 2): "

if "%choice%"=="1" (
    echo Installing local dependencies (OpenAI only)...
    pip install -r requirements-local.txt
) else if "%choice%"=="2" (
    echo Installing full dependencies (OpenAI + Snowflake)...
    pip install -r requirements.txt
) else (
    echo Invalid choice. Please run the script again and choose 1 or 2.
    pause
    exit /b 1
)

REM Create .env file if it doesn't exist
if not exist ".env" (
    echo Creating .env file from template...
    copy .env.example .env
    echo Please edit .env file and add your API keys!
) else (
    echo .env file already exists
)

echo.
echo Setup complete!
echo.
echo Next steps:
echo 1. Edit .env file and add your API keys
echo 2. Activate the virtual environment: venv\Scripts\activate.bat
echo 3. Run the application: streamlit run app.py
echo.
echo To deactivate the virtual environment later, use: deactivate
echo.
pause
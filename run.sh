#!/bin/bash

# Run script for AI Chat Application
# This script activates the virtual environment and starts the Streamlit app

set -e  # Exit on error

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}🚀 Starting AI Chat Application...${NC}"

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo -e "${RED}❌ Virtual environment not found!${NC}"
    echo "Please run ./setup.sh first to create the virtual environment."
    exit 1
fi

# Check if .env file exists
if [ ! -f ".env" ]; then
    echo -e "${RED}❌ .env file not found!${NC}"
    echo "Creating .env from template..."
    cp .env.example .env
    echo -e "${BLUE}⚠️  Please edit .env file and add your API keys!${NC}"
    exit 1
fi

# Activate virtual environment
echo -e "${GREEN}✓ Activating virtual environment...${NC}"
source venv/bin/activate

# Check if dependencies are installed
if ! python -c "import streamlit" 2>/dev/null; then
    echo -e "${RED}❌ Dependencies not installed!${NC}"
    echo "Installing dependencies..."
    pip install -r requirements-local.txt
fi

# Optional: Install watchdog for better performance
if ! python -c "import watchdog" 2>/dev/null; then
    echo -e "${BLUE}💡 Tip: Install watchdog for better file watching performance${NC}"
    echo "Run: pip install watchdog"
fi

# Display app info
echo -e "${GREEN}✓ Starting Streamlit application...${NC}"
echo -e "${BLUE}📱 The app will open in your browser automatically${NC}"
echo -e "${BLUE}📝 To stop the server, press Ctrl+C${NC}"
echo ""

# Run Streamlit
streamlit run app.py
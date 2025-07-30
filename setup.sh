#!/bin/bash

# Setup script for AI Chat Application
# This script creates a virtual environment and installs dependencies

set -e  # Exit on error

echo "🚀 Setting up AI Chat Application..."

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is not installed. Please install Python 3.8 or higher."
    exit 1
fi

# Get Python version
PYTHON_VERSION=$(python3 -c 'import sys; print(".".join(map(str, sys.version_info[:2])))')
echo "✓ Found Python $PYTHON_VERSION"

# Check if virtual environment exists
if [ -d "venv" ]; then
    echo "⚠️  Virtual environment already exists. Do you want to recreate it? (y/n)"
    read -r response
    if [[ "$response" =~ ^[Yy]$ ]]; then
        echo "🗑️  Removing existing virtual environment..."
        rm -rf venv
    else
        echo "📦 Using existing virtual environment..."
    fi
fi

# Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo "📦 Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
echo "🔧 Activating virtual environment..."
source venv/bin/activate

# Upgrade pip
echo "📈 Upgrading pip..."
pip install --upgrade pip

# Ask user about installation type
echo ""
echo "Which installation type do you want?"
echo "1) Local only (OpenAI support only)"
echo "2) Full installation (OpenAI + Snowflake support)"
echo ""
read -p "Enter your choice (1 or 2): " choice

case $choice in
    1)
        echo "📦 Installing local dependencies (OpenAI only)..."
        pip install -r requirements-local.txt
        ;;
    2)
        echo "📦 Installing full dependencies (OpenAI + Snowflake)..."
        pip install -r requirements.txt
        ;;
    *)
        echo "❌ Invalid choice. Please run the script again and choose 1 or 2."
        exit 1
        ;;
esac

# Create .env file if it doesn't exist
if [ ! -f ".env" ]; then
    echo "📝 Creating .env file from template..."
    cp .env.example .env
    echo "⚠️  Please edit .env file and add your API keys!"
else
    echo "✓ .env file already exists"
fi

echo ""
echo "✅ Setup complete!"
echo ""
echo "Next steps:"
echo "1. Edit .env file and add your API keys"
echo "2. Activate the virtual environment: source venv/bin/activate"
echo "3. Run the application: streamlit run app.py"
echo ""
echo "To deactivate the virtual environment later, use: deactivate"
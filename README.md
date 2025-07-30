# AI Chat Application with Streamlit

A multi-workspace AI chat application that can run both in Snowflake (using Cortex) and locally (using OpenAI).

## Features

- **Multiple Workspaces**: Organize your chats into different workspaces
- **Multiple Chat Sessions**: Create multiple chat sessions within each workspace
- **Dual LLM Support**: 
  - Snowflake Cortex (for Snowflake environments)
  - OpenAI API (for local development)
- **Model Selection**: Choose from available models for each provider
- **Chat History**: Maintains conversation context within each session
- **Clean Architecture**: Modular design with separate services and models

## Setup

### Quick Setup (Recommended)

We provide setup scripts that automatically create a virtual environment and install dependencies:

**For macOS/Linux:**
```bash
./setup.sh
```

**For Windows:**
```cmd
setup.bat
```

The setup script will:
- Create a Python virtual environment
- Ask if you want local-only (OpenAI) or full (OpenAI + Snowflake) installation
- Install the appropriate dependencies
- Create a `.env` file from the template

### Manual Setup

1. Clone the repository

2. Create a virtual environment:
   ```bash
   python -m venv venv
   
   # Activate on macOS/Linux:
   source venv/bin/activate
   
   # Activate on Windows:
   venv\Scripts\activate
   ```

3. Install dependencies:
   - For full installation (with Snowflake support):
     ```bash
     pip install -r requirements.txt
     ```
   - For local-only installation (OpenAI only, no Snowflake):
     ```bash
     pip install -r requirements-local.txt
     ```

4. Create a `.env` file based on `.env.example`:
   ```bash
   cp .env.example .env
   ```

### Configuration

Configure your credentials in `.env`:
- For Snowflake (optional): Set all `SNOWFLAKE_*` variables
- For OpenAI: Set `OPENAI_API_KEY`
- The app will work with just OpenAI configured

## Running the Application

### Locally with OpenAI
```bash
streamlit run app.py
```

### In Snowflake
Deploy to Snowflake Streamlit and ensure Cortex is enabled in your account.

## Project Structure

```
├── app.py                    # Main Streamlit application
├── config/
│   └── settings.py          # Configuration management
├── services/
│   ├── llm_interface.py     # Abstract LLM interface
│   ├── snowflake_service.py # Snowflake Cortex implementation
│   └── openai_service.py    # OpenAI REST API implementation
├── models/
│   ├── workspace.py         # Workspace data model
│   └── chat_session.py      # Chat session data model
└── utils/
    └── session_manager.py   # Streamlit session state management
```

## Available Models

### Snowflake Cortex
- llama2-70b-chat
- mistral-large
- mixtral-8x7b
- gemma-7b

### OpenAI
- gpt-3.5-turbo
- gpt-3.5-turbo-16k
- gpt-4
- gpt-4-turbo-preview

## Usage

1. Select or create a workspace
2. Create a new chat session within the workspace
3. Choose your LLM provider and model
4. Start chatting!

## Notes

- The application uses Streamlit's session state for persistence (data is not saved between sessions)
- OpenAI integration uses REST API calls (not the SDK) for flexibility
- All methods are in separate files following Snowflake Cortex structure
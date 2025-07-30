import os
from dotenv import load_dotenv
from typing import Optional

# Load environment variables
load_dotenv()


class Settings:
    """Application settings loaded from environment variables"""
    
    # Snowflake settings
    SNOWFLAKE_ACCOUNT: Optional[str] = os.getenv('SNOWFLAKE_ACCOUNT')
    SNOWFLAKE_USER: Optional[str] = os.getenv('SNOWFLAKE_USER')
    SNOWFLAKE_PASSWORD: Optional[str] = os.getenv('SNOWFLAKE_PASSWORD')
    SNOWFLAKE_WAREHOUSE: Optional[str] = os.getenv('SNOWFLAKE_WAREHOUSE')
    SNOWFLAKE_DATABASE: Optional[str] = os.getenv('SNOWFLAKE_DATABASE')
    SNOWFLAKE_SCHEMA: Optional[str] = os.getenv('SNOWFLAKE_SCHEMA')
    SNOWFLAKE_MODEL: str = os.getenv('SNOWFLAKE_MODEL', 'llama2-70b-chat')
    
    # OpenAI settings
    OPENAI_API_KEY: Optional[str] = os.getenv('OPENAI_API_KEY')
    OPENAI_MODEL: str = os.getenv('OPENAI_MODEL', 'gpt-3.5-turbo')
    
    # Default provider
    DEFAULT_PROVIDER: str = os.getenv('DEFAULT_PROVIDER', 'openai')
    
    @classmethod
    def is_snowflake_configured(cls) -> bool:
        """Check if Snowflake credentials are configured"""
        # First check if snowflake connector is available
        try:
            import snowflake.connector
        except ImportError:
            return False
        
        # Then check if all credentials are provided
        return all([
            cls.SNOWFLAKE_ACCOUNT,
            cls.SNOWFLAKE_USER,
            cls.SNOWFLAKE_PASSWORD,
            cls.SNOWFLAKE_WAREHOUSE,
            cls.SNOWFLAKE_DATABASE,
            cls.SNOWFLAKE_SCHEMA
        ])
    
    @classmethod
    def is_openai_configured(cls) -> bool:
        """Check if OpenAI credentials are configured"""
        return bool(cls.OPENAI_API_KEY)
    
    @classmethod
    def get_available_providers(cls) -> list:
        """Get list of configured providers"""
        providers = []
        if cls.is_snowflake_configured():
            providers.append('snowflake')
        if cls.is_openai_configured():
            providers.append('openai')
        return providers
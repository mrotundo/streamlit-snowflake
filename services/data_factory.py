from typing import Optional
from .data_interface import DataInterface
from .local_data_service import LocalDataService
from .snowflake_data_service import SnowflakeDataService
from config.settings import Settings


class DataServiceFactory:
    """Factory for creating the appropriate data service based on configuration"""
    
    @staticmethod
    def create_data_service(provider: Optional[str] = None) -> DataInterface:
        """
        Create a data service instance based on the provider or configuration
        
        Args:
            provider: 'local' or 'snowflake'. If None, uses configuration default.
        
        Returns:
            DataInterface implementation
        """
        # Determine which provider to use
        if provider is None:
            provider = Settings.DATA_PROVIDER if hasattr(Settings, 'DATA_PROVIDER') else 'local'
        
        provider = provider.lower()
        
        if provider == 'snowflake':
            # Check if Snowflake is configured
            if not Settings.is_snowflake_configured():
                print("Snowflake not configured, falling back to local data service")
                provider = 'local'
            else:
                return SnowflakeDataService(
                    account=Settings.SNOWFLAKE_ACCOUNT,
                    user=Settings.SNOWFLAKE_USER,
                    password=Settings.SNOWFLAKE_PASSWORD,
                    warehouse=Settings.SNOWFLAKE_WAREHOUSE,
                    database=Settings.SNOWFLAKE_DATABASE,
                    schema=Settings.SNOWFLAKE_SCHEMA
                )
        
        # Default to local data service
        db_path = getattr(Settings, 'LOCAL_DB_PATH', 'data/banking.db')
        return LocalDataService(db_path)
    
    @staticmethod
    def get_available_providers() -> list[str]:
        """Get list of available data providers"""
        providers = ['local']  # Local is always available
        
        if Settings.is_snowflake_configured():
            providers.append('snowflake')
        
        return providers
from typing import List, Dict, Optional
import json
from .llm_interface import LLMInterface

try:
    import snowflake.connector
    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False


class SnowflakeService(LLMInterface):
    """Snowflake Cortex LLM implementation"""
    
    AVAILABLE_MODELS = [
        'llama2-70b-chat',
        'mistral-large',
        'mixtral-8x7b',
        'gemma-7b'
    ]
    
    def __init__(
        self,
        account: str,
        user: str,
        password: str,
        warehouse: str,
        database: str,
        schema: str
    ):
        if not SNOWFLAKE_AVAILABLE:
            raise ImportError(
                "snowflake-connector-python is not installed. "
                "Install it with: pip install snowflake-connector-python"
            )
        
        self.connection_params = {
            'account': account,
            'user': user,
            'password': password,
            'warehouse': warehouse,
            'database': database,
            'schema': schema
        }
        self._connection = None
    
    def _get_connection(self):
        """Get or create a Snowflake connection"""
        if self._connection is None or self._connection.is_closed():
            self._connection = snowflake.connector.connect(**self.connection_params)
        return self._connection
    
    def complete(
        self,
        messages: List[Dict[str, str]],
        model: Optional[str] = None,
        temperature: float = 0.7,
        max_tokens: Optional[int] = None
    ) -> str:
        """Generate completion using Snowflake Cortex COMPLETE function"""
        if model is None:
            model = 'llama2-70b-chat'
        
        if model not in self.AVAILABLE_MODELS:
            raise ValueError(f"Model {model} not available. Choose from: {self.AVAILABLE_MODELS}")
        
        # Convert messages to prompt format
        prompt = self._format_messages(messages)
        
        # Build the SQL query with COMPLETE function
        query = f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            '{model}',
            '{prompt.replace("'", "''")}',
            {{
                'temperature': {temperature}
                {f", 'max_tokens': {max_tokens}" if max_tokens else ""}
            }}
        ) as response
        """
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(query)
            result = cursor.fetchone()
            cursor.close()
            
            if result and result[0]:
                return result[0]
            else:
                raise Exception("No response from Snowflake Cortex")
                
        except Exception as e:
            raise Exception(f"Snowflake Cortex error: {str(e)}")
    
    def _format_messages(self, messages: List[Dict[str, str]]) -> str:
        """Format messages for Snowflake Cortex prompt"""
        formatted_messages = []
        for msg in messages:
            role = msg['role']
            content = msg['content']
            
            if role == 'system':
                formatted_messages.append(f"System: {content}")
            elif role == 'user':
                formatted_messages.append(f"User: {content}")
            elif role == 'assistant':
                formatted_messages.append(f"Assistant: {content}")
        
        # Add prompt for assistant response
        formatted_messages.append("Assistant:")
        
        return "\n\n".join(formatted_messages)
    
    def get_available_models(self) -> List[str]:
        """Get list of available Snowflake Cortex models"""
        return self.AVAILABLE_MODELS.copy()
    
    def validate_connection(self) -> bool:
        """Test Snowflake connection"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT CURRENT_VERSION()")
            cursor.fetchone()
            cursor.close()
            return True
        except Exception:
            return False
    
    def __del__(self):
        """Close connection on cleanup"""
        if self._connection and not self._connection.is_closed():
            self._connection.close()
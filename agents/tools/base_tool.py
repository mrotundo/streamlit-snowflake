from abc import ABC, abstractmethod
from typing import Any, Dict, Optional


class BaseTool(ABC):
    """Abstract base class for all tools that agents can use"""
    
    def __init__(self, name: str, description: str):
        self.name = name
        self.description = description
    
    @abstractmethod
    def execute(self, **kwargs) -> Dict[str, Any]:
        """
        Execute the tool with given parameters.
        
        Returns:
            Dictionary containing:
                - success: bool
                - result: Any (the actual result)
                - error: Optional[str] (error message if failed)
        """
        pass
    
    @abstractmethod
    def get_parameters(self) -> Dict[str, Dict[str, str]]:
        """
        Get the parameters this tool accepts.
        
        Returns:
            Dictionary mapping parameter names to their descriptions and types
            Example: {
                "query": {"type": "string", "description": "SQL query to execute"},
                "limit": {"type": "integer", "description": "Maximum rows to return", "optional": True}
            }
        """
        pass
    
    def validate_parameters(self, **kwargs) -> tuple[bool, Optional[str]]:
        """Validate that required parameters are provided"""
        required_params = {
            name for name, info in self.get_parameters().items() 
            if not info.get("optional", False)
        }
        
        missing = required_params - set(kwargs.keys())
        if missing:
            return False, f"Missing required parameters: {', '.join(missing)}"
        
        return True, None
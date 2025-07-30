from abc import ABC, abstractmethod
from typing import List, Dict, Optional


class LLMInterface(ABC):
    """Abstract interface for LLM providers"""
    
    @abstractmethod
    def complete(
        self,
        messages: List[Dict[str, str]],
        model: Optional[str] = None,
        temperature: float = 0.7,
        max_tokens: Optional[int] = None
    ) -> str:
        """
        Generate a completion for the given messages.
        
        Args:
            messages: List of message dictionaries with 'role' and 'content'
            model: Model to use (provider-specific)
            temperature: Sampling temperature
            max_tokens: Maximum tokens to generate
            
        Returns:
            Generated text completion
        """
        pass
    
    @abstractmethod
    def get_available_models(self) -> List[str]:
        """Get list of available models for this provider"""
        pass
    
    @abstractmethod
    def validate_connection(self) -> bool:
        """Test if the connection to the provider is valid"""
        pass
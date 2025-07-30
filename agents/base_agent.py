from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from services.llm_interface import LLMInterface


class BaseAgent(ABC):
    """Abstract base class for all agents in the banking system"""
    
    def __init__(self, name: str, description: str):
        self.name = name
        self.description = description
        self._tools = []
    
    @abstractmethod
    def can_handle(self, query: str, llm_service: LLMInterface, model: str) -> tuple[bool, float]:
        """
        Determine if this agent can handle the given query.
        
        Args:
            query: The user's input query
            llm_service: LLM service for analysis if needed
            model: Model to use for analysis
            
        Returns:
            Tuple of (can_handle: bool, confidence: float between 0-1)
        """
        pass
    
    @abstractmethod
    def process(
        self, 
        query: str, 
        llm_service: LLMInterface, 
        model: str,
        conversation_history: List[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Process the query and return a response.
        
        Args:
            query: The user's input query
            llm_service: LLM service to use
            model: Model to use
            conversation_history: Previous messages in the conversation
            
        Returns:
            Dictionary containing:
                - response: The text response
                - data: Any data retrieved (optional)
                - visualizations: Any charts/graphs (optional)
                - metadata: Additional information (optional)
        """
        pass
    
    @abstractmethod
    def get_system_prompt(self) -> str:
        """Get the system prompt for this agent"""
        pass
    
    @property
    def capabilities(self) -> List[str]:
        """List of capabilities this agent provides"""
        return []
    
    def register_tool(self, tool):
        """Register a tool for this agent to use"""
        self._tools.append(tool)
    
    def get_tools(self):
        """Get all registered tools"""
        return self._tools
    
    def format_response(
        self, 
        text: str, 
        data: Optional[Any] = None,
        visualizations: Optional[List[Any]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Helper method to format agent responses consistently"""
        response = {
            "response": text,
            "agent": self.name
        }
        
        if data is not None:
            response["data"] = data
            
        if visualizations:
            response["visualizations"] = visualizations
            
        if metadata:
            response["metadata"] = metadata
            
        return response
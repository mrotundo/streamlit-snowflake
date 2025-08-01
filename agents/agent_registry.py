from typing import Dict, Type, Optional
from .base_agent import BaseAgent
from .router import AgentRouter
import importlib
import os


class AgentRegistry:
    """Central registry for all agents with auto-discovery"""
    
    _instance = None
    _agents: Dict[str, Type[BaseAgent]] = {}
    _router: Optional[AgentRouter] = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(AgentRegistry, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        self._initialized = True
        self._router = AgentRouter()
        self._discover_agents()
    
    def _discover_agents(self):
        """Auto-discover and load all agent implementations"""
        # Import banking agents
        from .banking.loan_agent import LoanAgent
        from .banking.deposit_agent import DepositAgent
        from .banking.customer_agent import CustomerAgent
        from .banking.uncertain_agent import UncertainAgent
        from .banking.data_status_agent import DataStatusAgent
        
        # Register discovered agents
        self.register_agent_class("LoanAgent", LoanAgent)
        self.register_agent_class("DepositAgent", DepositAgent)
        self.register_agent_class("CustomerAgent", CustomerAgent)
        self.register_agent_class("DataStatusAgent", DataStatusAgent)
        self.register_agent_class("UncertainAgent", UncertainAgent)
    
    def register_agent_class(self, name: str, agent_class: Type[BaseAgent]):
        """Register an agent class"""
        self._agents[name] = agent_class
    
    def create_agent(self, name: str) -> Optional[BaseAgent]:
        """Create an instance of a registered agent"""
        agent_class = self._agents.get(name)
        if agent_class:
            return agent_class()
        return None
    
    def get_router(self) -> AgentRouter:
        """Get the configured router with all agents"""
        if not self._router._agents:  # If router doesn't have agents yet
            # Create instances of all registered agents
            for name, agent_class in self._agents.items():
                agent_instance = agent_class()
                # UncertainAgent is the default for unclear queries
                is_default = (name == "UncertainAgent")
                self._router.register_agent(agent_instance, is_default=is_default)
        
        return self._router
    
    def get_all_agent_names(self) -> list[str]:
        """Get names of all registered agents"""
        return list(self._agents.keys())
    
    def get_agent_info(self) -> Dict[str, Dict[str, str]]:
        """Get information about all registered agents"""
        info = {}
        for name, agent_class in self._agents.items():
            agent_instance = agent_class()
            info[name] = {
                "name": agent_instance.name,
                "description": agent_instance.description,
                "capabilities": agent_instance.capabilities
            }
        return info


# Global registry instance
agent_registry = AgentRegistry()
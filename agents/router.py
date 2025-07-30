from typing import List, Dict, Optional, Tuple
from services.llm_interface import LLMInterface
from .base_agent import BaseAgent
import json


class AgentRouter:
    """Routes queries to the appropriate banking agent"""
    
    def __init__(self):
        self._agents: List[BaseAgent] = []
        self._default_agent: Optional[BaseAgent] = None
    
    def register_agent(self, agent: BaseAgent, is_default: bool = False):
        """Register an agent with the router"""
        self._agents.append(agent)
        if is_default:
            self._default_agent = agent
    
    def route(
        self, 
        query: str, 
        llm_service: LLMInterface, 
        model: str
    ) -> Tuple[BaseAgent, float]:
        """
        Route a query to the most appropriate agent.
        
        Returns:
            Tuple of (selected_agent, confidence_score)
        """
        # First, try to route using agent-specific logic
        best_agent = None
        best_confidence = 0.0
        
        for agent in self._agents:
            can_handle, confidence = agent.can_handle(query, llm_service, model)
            if can_handle and confidence > best_confidence:
                best_agent = agent
                best_confidence = confidence
        
        # If no agent claimed high confidence, use LLM-based routing
        if best_confidence < 0.7:
            llm_agent, llm_confidence = self._llm_route(query, llm_service, model)
            if llm_confidence > best_confidence:
                best_agent = llm_agent
                best_confidence = llm_confidence
        
        # If still no good match, use default agent
        if best_agent is None or best_confidence < 0.3:
            best_agent = self._default_agent
            best_confidence = 0.5
        
        return best_agent, best_confidence
    
    def _llm_route(
        self, 
        query: str, 
        llm_service: LLMInterface, 
        model: str
    ) -> Tuple[Optional[BaseAgent], float]:
        """Use LLM to determine the best agent for the query"""
        
        # Build agent descriptions
        agent_descriptions = []
        for agent in self._agents:
            agent_descriptions.append({
                "name": agent.name,
                "description": agent.description,
                "capabilities": agent.capabilities
            })
        
        routing_prompt = f"""You are a query router for a banking system. Analyze the user query and determine which agent should handle it.

Available agents:
{json.dumps(agent_descriptions, indent=2)}

User query: "{query}"

Respond with ONLY a JSON object in this format:
{{
    "agent_name": "name of the most appropriate agent",
    "confidence": 0.0 to 1.0,
    "reasoning": "brief explanation"
}}"""
        
        try:
            messages = [
                {"role": "system", "content": "You are a precise query routing system. Always respond with valid JSON."},
                {"role": "user", "content": routing_prompt}
            ]
            
            response = llm_service.complete(messages, model=model, temperature=0.1)
            
            # Parse response
            # Try to extract JSON from the response
            import re
            json_match = re.search(r'\{.*\}', response, re.DOTALL)
            if json_match:
                result = json.loads(json_match.group())
                agent_name = result.get("agent_name", "")
                confidence = float(result.get("confidence", 0.0))
                
                # Find the agent by name
                for agent in self._agents:
                    if agent.name.lower() == agent_name.lower():
                        return agent, confidence
        except Exception as e:
            # If routing fails, we'll fall back to default
            pass
        
        return None, 0.0
    
    def get_all_agents(self) -> List[BaseAgent]:
        """Get all registered agents"""
        return self._agents.copy()
    
    def get_agent_by_name(self, name: str) -> Optional[BaseAgent]:
        """Get a specific agent by name"""
        for agent in self._agents:
            if agent.name.lower() == name.lower():
                return agent
        return None
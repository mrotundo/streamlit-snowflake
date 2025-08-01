from typing import List, Dict, Any, Optional
from agents.base_agent import BaseAgent
from services.llm_interface import LLMInterface
import json


class UncertainAgent(BaseAgent):
    """Agent that handles unclear queries and helps route users to the appropriate specialist"""
    
    def __init__(self):
        super().__init__(
            name="UncertainAgent",
            description="Handles unclear queries and helps users clarify their needs"
        )
    
    @property
    def capabilities(self) -> List[str]:
        return [
            "Query clarification",
            "Intent disambiguation",
            "Agent recommendation",
            "General assistance"
        ]
    
    def can_handle(self, query: str, llm_service: LLMInterface, model: str) -> tuple[bool, float]:
        """This agent handles all queries that other agents can't confidently handle"""
        # Return a very low confidence so this agent is only used as a fallback
        # when no other agent can handle the query with confidence
        return True, 0.1
    
    def get_system_prompt(self) -> str:
        return """You are a helpful banking assistant designed to clarify unclear requests and guide users to the right specialist.

When a user's request is unclear:
1. Acknowledge that you need clarification
2. Identify possible interpretations of their request
3. Ask specific clarifying questions
4. Explain the available specialists and what they can help with

Available specialists:
- **LoanAgent**: Handles all loan-related queries (mortgages, personal loans, interest rates, applications, refinancing)
- **DepositAgent**: Manages deposit accounts (checking, savings, balances, transactions, interest earnings)
- **CustomerAgent**: Assists with customer analytics (segmentation, lifetime value, satisfaction, demographics)

Be friendly, professional, and guide users toward getting the help they need."""
    
    def process(
        self, 
        query: str, 
        llm_service: LLMInterface, 
        model: str,
        conversation_history: List[Dict[str, str]] = None,
        debug_callback: callable = None,
        data_service: Optional[Any] = None
    ) -> Dict[str, Any]:
        """Process unclear queries by asking for clarification"""
        
        if debug_callback:
            debug_callback("UncertainAgent processing unclear query")
        
        # Don't use the planning system for this agent - just use direct LLM
        messages = [{"role": "system", "content": self.get_system_prompt()}]
        
        if conversation_history:
            messages.extend(conversation_history)
        
        # Create a clarification prompt
        clarification_prompt = f"""The user asked: "{query}"

This request is unclear. Please:
1. Acknowledge that you need more information
2. Identify what they might be asking about
3. Ask clarifying questions
4. Suggest which specialist could help once clarified

Available specialists:
- LoanAgent: For loans, mortgages, interest rates, loan applications
- DepositAgent: For accounts, balances, transactions, savings
- CustomerAgent: For customer data, analytics, segmentation

Be helpful and guide them to the right specialist."""
        
        messages.append({"role": "user", "content": clarification_prompt})
        
        try:
            response_text = llm_service.complete(messages, model=model, temperature=0.7)
            
            # Also analyze the query to suggest potential agents
            agent_suggestions = self._analyze_query_for_suggestions(query, llm_service, model)
            
            return self.format_response(
                text=response_text,
                metadata={
                    "agent_type": "clarification_specialist",
                    "needs_clarification": True,
                    "potential_agents": agent_suggestions,
                    "original_query": query
                }
            )
            
        except Exception as e:
            if debug_callback:
                debug_callback(f"Error in UncertainAgent: {str(e)}", "ERROR")
            
            # Fallback response
            return self.format_response(
                text="""I'm having trouble understanding your request. Could you please provide more details? 

I can help you with:
- **Loans**: Interest rates, applications, mortgages, refinancing
- **Deposits**: Account balances, transactions, savings accounts
- **Customers**: Analytics, segmentation, demographics

What would you like to know more about?""",
                metadata={
                    "agent_type": "clarification_specialist",
                    "needs_clarification": True,
                    "error": str(e)
                }
            )
    
    def _analyze_query_for_suggestions(self, query: str, llm_service: LLMInterface, model: str) -> List[str]:
        """Analyze the query to suggest potential agents"""
        suggestions = []
        query_lower = query.lower()
        
        # Simple keyword matching for suggestions
        loan_keywords = ["loan", "borrow", "mortgage", "interest", "rate", "refinance"]
        deposit_keywords = ["account", "balance", "deposit", "saving", "checking", "transaction"]
        customer_keywords = ["customer", "client", "segment", "analytics", "demographic"]
        
        if any(keyword in query_lower for keyword in loan_keywords):
            suggestions.append("LoanAgent")
        if any(keyword in query_lower for keyword in deposit_keywords):
            suggestions.append("DepositAgent")
        if any(keyword in query_lower for keyword in customer_keywords):
            suggestions.append("CustomerAgent")
        
        # If no suggestions from keywords, suggest all
        if not suggestions:
            suggestions = ["LoanAgent", "DepositAgent", "CustomerAgent"]
        
        return suggestions
    
    def create_plan(
        self,
        query: str,
        llm_service: LLMInterface,
        model: str,
        conversation_history: Optional[List[Dict[str, str]]] = None
    ) -> Dict[str, Any]:
        """Create a simple plan for clarification - not using the full planning system"""
        return {
            "goal": "Clarify user intent and guide to appropriate specialist",
            "steps": [
                {
                    "step": 1,
                    "tool": "DirectResponse",
                    "description": "Ask clarifying questions",
                    "inputs": {
                        "query": query
                    },
                    "output_key": "clarification"
                }
            ],
            "adaptations": {
                "no_data": "Provide general help options",
                "error": "Show available specialists"
            }
        }
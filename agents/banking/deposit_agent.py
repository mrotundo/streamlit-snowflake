from typing import List, Dict, Any
from agents.base_agent import BaseAgent
from services.llm_interface import LLMInterface


class DepositAgent(BaseAgent):
    """Agent specialized in deposit accounts, transactions, and balance management"""
    
    def __init__(self):
        super().__init__(
            name="DepositAgent",
            description="Handles deposit accounts, transactions, balances, and savings analysis"
        )
    
    @property
    def capabilities(self) -> List[str]:
        return [
            "Account balance inquiries",
            "Transaction history analysis",
            "Deposit growth trends",
            "Savings account optimization",
            "CD (Certificate of Deposit) rates",
            "Account type comparisons",
            "Interest earnings calculations",
            "Deposit insurance information",
            "Cash flow analysis"
        ]
    
    def can_handle(self, query: str, llm_service: LLMInterface, model: str) -> tuple[bool, float]:
        """Determine if this agent can handle deposit-related queries"""
        
        deposit_keywords = [
            "deposit", "saving", "account", "balance", "transaction",
            "checking", "withdrawal", "transfer", "interest", "cd",
            "certificate", "atm", "branch", "statement", "overdraft",
            "minimum balance", "monthly fee", "direct deposit"
        ]
        
        query_lower = query.lower()
        
        # Check for keyword matches
        keyword_matches = sum(1 for keyword in deposit_keywords if keyword in query_lower)
        
        if keyword_matches >= 2:
            return True, 0.9
        elif keyword_matches == 1:
            return True, 0.6
        
        # Use LLM for more nuanced classification
        classification_prompt = f"""Determine if this query is related to deposit accounts, savings, checking, or account management.
Query: "{query}"

Respond with ONLY a number between 0 and 1 indicating confidence that this is deposit-related."""
        
        try:
            messages = [{"role": "user", "content": classification_prompt}]
            response = llm_service.complete(messages, model=model, temperature=0.1, max_tokens=10)
            confidence = float(response.strip())
            return confidence > 0.5, confidence
        except:
            return False, 0.0
    
    def get_system_prompt(self) -> str:
        return """You are a specialized banking deposit accounts AI assistant. Your expertise includes:
- Deposit account management (checking, savings, CDs, money market)
- Transaction analysis and patterns
- Balance optimization strategies
- Interest rate maximization
- Account fee structures and minimization
- FDIC insurance and regulations
- Cash management best practices

When answering questions:
1. Provide clear explanations of account features and benefits
2. Calculate interest earnings when relevant
3. Suggest account optimization strategies
4. Consider regulatory requirements (FDIC limits, etc.)
5. Help identify unusual transaction patterns

Always maintain professional banking standards and help customers maximize their deposit value."""
    
    def process(
        self, 
        query: str, 
        llm_service: LLMInterface, 
        model: str,
        conversation_history: List[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """Process deposit-related queries"""
        
        messages = [{"role": "system", "content": self.get_system_prompt()}]
        
        if conversation_history:
            messages.extend(conversation_history)
        
        messages.append({"role": "user", "content": query})
        
        # Check if query might need data or visualization
        needs_data = any(keyword in query.lower() for keyword in [
            "balance", "transaction", "history", "trend", "analysis",
            "statement", "activity", "summary", "report"
        ])
        
        if needs_data:
            enhanced_query = f"""{query}

Note: As a deposit specialist, I can help with:
- Account balance analysis across all deposit types
- Transaction pattern identification
- Deposit growth trends over time
- Interest earnings optimization
- Fee analysis and reduction strategies

Please specify if you need analysis for specific accounts or time periods."""
            
            messages[-1]["content"] = enhanced_query
        
        response_text = llm_service.complete(messages, model=model)
        
        result = self.format_response(
            text=response_text,
            metadata={
                "agent_type": "deposit_specialist",
                "capabilities_used": self._identify_used_capabilities(query)
            }
        )
        
        # Add sample data for common queries
        if "balance" in query.lower() or "account" in query.lower():
            result["data"] = {
                "sample_account_summary": {
                    "total_deposits": "$5.2M",
                    "number_of_accounts": 3500,
                    "average_balance": "$1,485",
                    "accounts_by_type": {
                        "checking": "45%",
                        "savings": "35%",
                        "CD": "15%",
                        "money_market": "5%"
                    }
                }
            }
            result["suggested_visualizations"] = [
                "Account balance distribution",
                "Deposit growth trend",
                "Account type breakdown"
            ]
        
        return result
    
    def _identify_used_capabilities(self, query: str) -> List[str]:
        """Identify which capabilities might be used for this query"""
        used = []
        query_lower = query.lower()
        
        capability_keywords = {
            "Account balance inquiries": ["balance", "how much", "account total"],
            "Transaction history analysis": ["transaction", "history", "activity"],
            "Deposit growth trends": ["growth", "trend", "increase"],
            "Interest earnings calculations": ["interest", "earnings", "yield"],
            "Cash flow analysis": ["cash flow", "inflow", "outflow"]
        }
        
        for capability, keywords in capability_keywords.items():
            if any(keyword in query_lower for keyword in keywords):
                used.append(capability)
        
        return used
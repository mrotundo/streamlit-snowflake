from typing import List, Dict, Any
from agents.base_agent import BaseAgent
from services.llm_interface import LLMInterface
import json


class LoanAgent(BaseAgent):
    """Agent specialized in loan-related queries and analysis"""
    
    def __init__(self):
        super().__init__(
            name="LoanAgent",
            description="Handles loan inquiries, applications, rates, and portfolio analysis"
        )
    
    @property
    def capabilities(self) -> List[str]:
        return [
            "Loan portfolio analysis",
            "Interest rate calculations",
            "Loan eligibility assessment",
            "Risk assessment summaries",
            "Loan performance metrics",
            "Default rate analysis",
            "Loan application status",
            "Refinancing options"
        ]
    
    def can_handle(self, query: str, llm_service: LLMInterface, model: str) -> tuple[bool, float]:
        """Determine if this agent can handle the loan-related query"""
        
        # Keywords that indicate loan-related queries
        loan_keywords = [
            "loan", "mortgage", "interest rate", "apr", "principal",
            "lending", "borrow", "credit", "refinance", "amortization",
            "default", "delinquency", "origination", "underwriting",
            "collateral", "debt", "repayment", "installment"
        ]
        
        query_lower = query.lower()
        
        # Check for keyword matches
        keyword_matches = sum(1 for keyword in loan_keywords if keyword in query_lower)
        
        if keyword_matches >= 2:
            return True, 0.9
        elif keyword_matches == 1:
            return True, 0.7
        
        # Use LLM for more nuanced classification
        classification_prompt = f"""Determine if this query is related to loans, lending, or credit products.
Query: "{query}"

Respond with ONLY a number between 0 and 1 indicating confidence that this is loan-related."""
        
        try:
            messages = [{"role": "user", "content": classification_prompt}]
            response = llm_service.complete(messages, model=model, temperature=0.1, max_tokens=10)
            confidence = float(response.strip())
            return confidence > 0.5, confidence
        except:
            return False, 0.0
    
    def get_system_prompt(self) -> str:
        return """You are a specialized banking loan analyst AI assistant. Your expertise includes:
- Loan portfolio analysis and performance metrics
- Interest rate calculations and comparisons
- Risk assessment and credit analysis
- Loan eligibility and underwriting criteria
- Regulatory compliance for lending
- Various loan types (mortgages, personal loans, business loans, etc.)

When answering questions:
1. Provide accurate financial calculations when needed
2. Explain loan terms and concepts clearly
3. Consider risk factors and regulatory requirements
4. Use appropriate financial terminology
5. If data is needed, indicate what specific data would be helpful

Always maintain professional banking standards and provide actionable insights."""
    
    def process(
        self, 
        query: str, 
        llm_service: LLMInterface, 
        model: str,
        conversation_history: List[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """Process loan-related queries"""
        
        # Build messages with loan-specific context
        messages = [{"role": "system", "content": self.get_system_prompt()}]
        
        # Add conversation history if provided
        if conversation_history:
            messages.extend(conversation_history)
        
        # Add the current query
        messages.append({"role": "user", "content": query})
        
        # Check if query might need data or visualization
        needs_data = any(keyword in query.lower() for keyword in [
            "portfolio", "analysis", "trend", "performance", "distribution",
            "breakdown", "summary", "report", "metrics", "statistics"
        ])
        
        if needs_data:
            # Add a note about data capabilities
            enhanced_query = f"""{query}

Note: As a loan specialist, I can help with:
- Loan portfolio analysis and breakdowns
- Performance metrics and trends
- Risk assessment summaries
- Interest rate comparisons
- Default rate analysis

If you need specific data analysis, please specify the metrics or time period you're interested in."""
            
            messages[-1]["content"] = enhanced_query
        
        # Get response from LLM
        response_text = llm_service.complete(messages, model=model)
        
        # Format the response
        result = self.format_response(
            text=response_text,
            metadata={
                "agent_type": "loan_specialist",
                "capabilities_used": self._identify_used_capabilities(query)
            }
        )
        
        # Add sample data structure for common queries (in real implementation, this would query actual data)
        if "portfolio" in query.lower() and "analysis" in query.lower():
            result["data"] = {
                "sample_portfolio_metrics": {
                    "total_loans": 10000,
                    "total_value": "$2.5B",
                    "average_loan_size": "$250,000",
                    "current_default_rate": "2.3%",
                    "average_interest_rate": "5.75%"
                }
            }
            result["suggested_visualizations"] = [
                "Loan distribution by type",
                "Default rate trend over time",
                "Interest rate comparison by loan category"
            ]
        
        return result
    
    def _identify_used_capabilities(self, query: str) -> List[str]:
        """Identify which capabilities might be used for this query"""
        used = []
        query_lower = query.lower()
        
        capability_keywords = {
            "Loan portfolio analysis": ["portfolio", "breakdown", "distribution"],
            "Interest rate calculations": ["interest", "rate", "apr"],
            "Risk assessment summaries": ["risk", "assessment", "credit score"],
            "Loan performance metrics": ["performance", "metrics", "kpi"],
            "Default rate analysis": ["default", "delinquency", "non-performing"]
        }
        
        for capability, keywords in capability_keywords.items():
            if any(keyword in query_lower for keyword in keywords):
                used.append(capability)
        
        return used
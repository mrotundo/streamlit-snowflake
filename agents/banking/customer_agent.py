from typing import List, Dict, Any
from agents.base_agent import BaseAgent
from services.llm_interface import LLMInterface


class CustomerAgent(BaseAgent):
    """Agent specialized in customer profiles, segmentation, and behavior analysis"""
    
    def __init__(self):
        super().__init__(
            name="CustomerAgent",
            description="Handles customer analytics, segmentation, satisfaction, and general banking queries"
        )
    
    @property
    def capabilities(self) -> List[str]:
        return [
            "Customer segmentation analysis",
            "Customer lifetime value (CLV) calculation",
            "Churn prediction and prevention",
            "Customer satisfaction metrics",
            "Cross-sell/up-sell opportunities",
            "Customer demographics analysis",
            "Banking product recommendations",
            "Customer journey mapping",
            "General banking inquiries"
        ]
    
    def can_handle(self, query: str, llm_service: LLMInterface, model: str) -> tuple[bool, float]:
        """Determine if this agent can handle customer-related queries"""
        
        customer_keywords = [
            "customer", "client", "user", "member", "satisfaction",
            "churn", "retention", "lifetime value", "clv", "segment",
            "demographic", "behavior", "cross-sell", "upsell",
            "experience", "journey", "persona", "profile"
        ]
        
        query_lower = query.lower()
        
        # Check for keyword matches
        keyword_matches = sum(1 for keyword in customer_keywords if keyword in query_lower)
        
        if keyword_matches >= 2:
            return True, 0.85
        elif keyword_matches == 1:
            return True, 0.6
        
        # This agent also handles general queries as the default
        # If no other specific keywords, still handle with lower confidence
        return True, 0.3
    
    def get_system_prompt(self) -> str:
        return """You are a specialized banking customer analytics AI assistant. Your expertise includes:
- Customer segmentation and profiling
- Customer lifetime value analysis
- Churn prediction and retention strategies
- Customer satisfaction and NPS analysis
- Cross-sell and up-sell opportunity identification
- Demographic and behavioral analysis
- General banking knowledge and assistance

When answering questions:
1. Provide insights based on customer data patterns
2. Suggest actionable strategies for customer engagement
3. Identify opportunities for improving customer experience
4. Use data-driven approaches for recommendations
5. Consider privacy and regulatory requirements

For general banking queries, provide helpful and accurate information while maintaining a professional tone."""
    
    def process(
        self, 
        query: str, 
        llm_service: LLMInterface, 
        model: str,
        conversation_history: List[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """Process customer-related queries"""
        
        messages = [{"role": "system", "content": self.get_system_prompt()}]
        
        if conversation_history:
            messages.extend(conversation_history)
        
        messages.append({"role": "user", "content": query})
        
        # Check if query is analytical in nature
        is_analytical = any(keyword in query.lower() for keyword in [
            "analysis", "analyze", "segment", "trend", "pattern",
            "metric", "kpi", "performance", "report", "insight"
        ])
        
        if is_analytical:
            enhanced_query = f"""{query}

Note: As a customer analytics specialist, I can help with:
- Customer segmentation analysis
- Lifetime value calculations
- Churn risk assessment
- Satisfaction metrics and NPS
- Cross-sell opportunity identification
- Demographic insights

Please specify if you need analysis for specific customer segments or time periods."""
            
            messages[-1]["content"] = enhanced_query
        
        response_text = llm_service.complete(messages, model=model)
        
        result = self.format_response(
            text=response_text,
            metadata={
                "agent_type": "customer_specialist",
                "capabilities_used": self._identify_used_capabilities(query)
            }
        )
        
        # Add sample data for customer analytics queries
        if any(term in query.lower() for term in ["segment", "customer analysis", "demographics"]):
            result["data"] = {
                "sample_customer_metrics": {
                    "total_customers": 50000,
                    "average_products_per_customer": 2.3,
                    "average_clv": "$15,000",
                    "churn_rate": "8.5%",
                    "nps_score": 42,
                    "segments": {
                        "high_value": "15%",
                        "growth": "25%",
                        "maintain": "40%",
                        "at_risk": "20%"
                    }
                }
            }
            result["suggested_visualizations"] = [
                "Customer segmentation pie chart",
                "CLV distribution histogram",
                "Churn risk heat map",
                "Product adoption by segment"
            ]
        
        return result
    
    def _identify_used_capabilities(self, query: str) -> List[str]:
        """Identify which capabilities might be used for this query"""
        used = []
        query_lower = query.lower()
        
        capability_keywords = {
            "Customer segmentation analysis": ["segment", "group", "cluster", "categorize"],
            "Customer lifetime value (CLV) calculation": ["lifetime value", "clv", "ltv"],
            "Churn prediction and prevention": ["churn", "retention", "leaving", "attrition"],
            "Customer satisfaction metrics": ["satisfaction", "nps", "happy", "experience"],
            "Cross-sell/up-sell opportunities": ["cross-sell", "upsell", "recommend", "additional"]
        }
        
        for capability, keywords in capability_keywords.items():
            if any(keyword in query_lower for keyword in keywords):
                used.append(capability)
        
        # If no specific capabilities identified, mark as general inquiry
        if not used:
            used.append("General banking inquiries")
        
        return used
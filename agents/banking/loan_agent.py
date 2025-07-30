from typing import List, Dict, Any, Optional
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
    
    def create_plan(
        self,
        query: str,
        llm_service: LLMInterface,
        model: str,
        conversation_history: Optional[List[Dict[str, str]]] = None
    ) -> Dict[str, Any]:
        """Create an execution plan for loan-related queries"""
        
        planning_prompt = f"""Create an execution plan to answer this loan-related query: "{query}"

Available tools:
1. SynthesizeQuery - Requires: requirements (string), query_type (string, optional)
2. RunQuery - Requires: query (dict)
3. ProvideAnalysis - Requires: data (dict), question (string), analysis_type (string, optional)

Create a JSON plan with:
- goal: What we're trying to achieve
- steps: Array of steps, each with:
  - step: Step number
  - tool: Tool name to use
  - description: What this step does
  - inputs: Tool inputs (can reference previous outputs with ${{output_key}})
  - output_key: Key to store this step's output
- adaptations: How to handle errors or missing data

IMPORTANT: If the query involves comparing time periods (e.g., "this quarter vs last year"), create separate steps to:
1. Get data for the current period
2. Get data for the comparison period
3. Analyze the comparison between periods

Focus on getting the right loan data to answer the question accurately.

Example step structure:
{{
  "step": 1,
  "tool": "SynthesizeQuery",
  "description": "Convert requirements to query",
  "inputs": {{
    "requirements": "Get loan portfolio data",
    "query_type": "loan"
  }},
  "output_key": "loan_query"
}}

Respond with ONLY valid JSON."""

        messages = [
            {"role": "system", "content": "You are a loan analysis planning expert. Create detailed execution plans."},
            {"role": "user", "content": planning_prompt}
        ]
        
        if conversation_history:
            # Add context about previous conversation
            context = "Previous conversation context:\n"
            for msg in conversation_history[-3:]:  # Last 3 messages
                context += f"{msg['role']}: {msg['content'][:100]}...\n"
            messages[0]["content"] += f"\n\n{context}"
        
        try:
            response = llm_service.complete(messages, model=model, temperature=0.1)
            
            # Parse JSON response
            try:
                plan = json.loads(response)
            except json.JSONDecodeError:
                # Extract JSON from response
                import re
                json_match = re.search(r'\{.*\}', response, re.DOTALL)
                if json_match:
                    plan = json.loads(json_match.group())
                else:
                    plan = self._create_default_plan(query)
            
            # Ensure plan has required structure
            if "goal" not in plan:
                plan["goal"] = f"Answer loan query: {query}"
            if "steps" not in plan or not plan["steps"]:
                plan = self._create_default_plan(query)
            
            return plan
            
        except Exception as e:
            return self._create_default_plan(query)
    
    def _create_default_plan(self, query: str) -> Dict[str, Any]:
        """Create a default plan when automatic planning fails"""
        
        # Determine what type of loan query this is
        query_lower = query.lower()
        
        # Check if this is a time comparison query
        if any(phrase in query_lower for phrase in ["compared to", "vs", "versus", "last year", "last quarter", "year over year", "quarter over quarter"]):
            # Time comparison plan
            return {
                "goal": f"Compare loan performance across time periods: {query}",
                "steps": [
                    {
                        "step": 1,
                        "tool": "SynthesizeQuery",
                        "description": "Create query for current period loan data",
                        "inputs": {
                            "requirements": f"Get current quarter (Q4 2024) loan data for: {query}",
                            "query_type": "loan"
                        },
                        "output_key": "current_period_query"
                    },
                    {
                        "step": 2,
                        "tool": "RunQuery",
                        "description": "Get current period loan data",
                        "inputs": {
                            "query": "${current_period_query.query}"
                        },
                        "output_key": "current_period_data"
                    },
                    {
                        "step": 3,
                        "tool": "SynthesizeQuery",
                        "description": "Create query for comparison period loan data",
                        "inputs": {
                            "requirements": f"Get comparison period (last year/quarter) loan data for: {query}",
                            "query_type": "loan"
                        },
                        "output_key": "comparison_period_query"
                    },
                    {
                        "step": 4,
                        "tool": "RunQuery",
                        "description": "Get comparison period loan data",
                        "inputs": {
                            "query": "${comparison_period_query.query}"
                        },
                        "output_key": "comparison_period_data"
                    },
                    {
                        "step": 5,
                        "tool": "ProvideAnalysis",
                        "description": "Compare loan performance between periods",
                        "inputs": {
                            "data": "${current_period_data}",
                            "question": query,
                            "analysis_type": "time_comparison",
                            "comparison_data": "${comparison_period_data}"
                        },
                        "output_key": "analysis"
                    }
                ],
                "adaptations": {
                    "no_data": "Explain typical loan performance trends",
                    "error": "Provide general insights on loan performance comparisons"
                }
            }
        
        elif any(word in query_lower for word in ["portfolio", "analysis", "performance", "trend"]):
            # Portfolio analysis plan
            return {
                "goal": f"Analyze loan portfolio to answer: {query}",
                "steps": [
                    {
                        "step": 1,
                        "tool": "SynthesizeQuery",
                        "description": "Convert requirements to loan data query",
                        "inputs": {
                            "requirements": query,
                            "query_type": "loan"
                        },
                        "output_key": "loan_query"
                    },
                    {
                        "step": 2,
                        "tool": "RunQuery",
                        "description": "Execute query to get loan portfolio data",
                        "inputs": {
                            "query": "${loan_query.query}"
                        },
                        "output_key": "loan_data"
                    },
                    {
                        "step": 3,
                        "tool": "ProvideAnalysis",
                        "description": "Analyze loan data to answer the question",
                        "inputs": {
                            "data": "${loan_data}",
                            "question": query,
                            "analysis_type": "portfolio_analysis"
                        },
                        "output_key": "analysis"
                    }
                ],
                "adaptations": {
                    "no_data": "Explain what loan data would be needed",
                    "error": "Provide general loan portfolio insights"
                }
            }
        
        elif any(word in query_lower for word in ["rate", "interest", "apr"]):
            # Interest rate analysis plan
            return {
                "goal": f"Analyze interest rates to answer: {query}",
                "steps": [
                    {
                        "step": 1,
                        "tool": "SynthesizeQuery",
                        "description": "Create query for interest rate data",
                        "inputs": {
                            "requirements": f"Get loan interest rate data for: {query}",
                            "query_type": "loan"
                        },
                        "output_key": "rate_query"
                    },
                    {
                        "step": 2,
                        "tool": "RunQuery",
                        "description": "Get interest rate data",
                        "inputs": {
                            "query": "${rate_query.query}"
                        },
                        "output_key": "rate_data"
                    },
                    {
                        "step": 3,
                        "tool": "ProvideAnalysis",
                        "description": "Analyze rates and provide insights",
                        "inputs": {
                            "data": "${rate_data}",
                            "question": query,
                            "analysis_type": "rate_comparison"
                        },
                        "output_key": "analysis"
                    }
                ],
                "adaptations": {
                    "no_data": "Provide general interest rate guidance",
                    "error": "Explain typical rate ranges"
                }
            }
        
        else:
            # General loan query plan
            return {
                "goal": f"Answer loan question: {query}",
                "steps": [
                    {
                        "step": 1,
                        "tool": "SynthesizeQuery",
                        "description": "Understand data needs",
                        "inputs": {
                            "requirements": query,
                            "query_type": "loan"
                        },
                        "output_key": "data_query"
                    },
                    {
                        "step": 2,
                        "tool": "RunQuery",
                        "description": "Get relevant loan data",
                        "inputs": {
                            "query": "${data_query.query}"
                        },
                        "output_key": "loan_info"
                    },
                    {
                        "step": 3,
                        "tool": "ProvideAnalysis",
                        "description": "Provide comprehensive answer",
                        "inputs": {
                            "data": "${loan_info}",
                            "question": query,
                            "analysis_type": "general"
                        },
                        "output_key": "analysis"
                    }
                ],
                "adaptations": {
                    "no_data": "Provide general loan guidance",
                    "error": "Offer alternative information sources"
                }
            }
    
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
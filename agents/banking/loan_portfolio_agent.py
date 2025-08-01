from typing import List, Dict, Any, Optional
from agents.base_agent import BaseAgent
from services.llm_interface import LLMInterface
from services.data_interface import DataInterface
import json


class LoanPortfolioAgent(BaseAgent):
    """Agent specialized in loan portfolio analysis with real database data"""
    
    def __init__(self, data_service: DataInterface):
        super().__init__(
            name="LoanPortfolioAgent",
            description="Performs loan portfolio analysis, risk assessment, and performance metrics using real data"
        )
        self.data_service = data_service
    
    def _initialize_tools(self, llm_service: LLMInterface, model: str):
        """Initialize loan portfolio specific tools"""
        # Clear existing tools first
        self._tools = []
        self.plan_executor.tools_registry = {}
        from agents.tools.banking.loan_query_tool import LoanQueryTool
        from agents.tools.banking.analyze_loan_portfolio_tool import AnalyzeLoanPortfolioTool
        
        # Register tools
        self.register_tool(LoanQueryTool(self.data_service))
        self.register_tool(AnalyzeLoanPortfolioTool(llm_service, model))
    
    @property
    def capabilities(self) -> List[str]:
        return [
            "Loan portfolio analysis with real-time data",
            "Risk assessment and concentration analysis",
            "Default and delinquency trend analysis",
            "Vintage performance tracking",
            "Interest rate and yield analysis",
            "Portfolio quality metrics",
            "Stress testing and scenario analysis",
            "Performance benchmarking and comparisons"
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
        return """You are a specialized loan portfolio analyst AI assistant with access to real banking data. Your expertise includes:
- Real-time loan portfolio analysis using actual loan data
- Risk assessment based on current portfolio composition
- Default and delinquency analysis from historical data
- Vintage performance tracking and cohort analysis
- Interest rate distribution and yield optimization
- Stress testing and scenario analysis
- Regulatory compliance metrics and reporting

When answering questions:
1. Always use real data from database queries
2. Provide specific metrics, rates, and dollar amounts
3. Base risk assessments on actual portfolio performance
4. Compare current metrics to historical trends
5. Highlight actionable insights from the data
6. Consider regulatory requirements (Basel III, CECL, etc.)

You have access to loan, customer, and transaction data. Use this to provide accurate, data-driven portfolio insights."""
    
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
1. LoanQuery - Requires: query_type (string), filters (dict, optional), time_period (dict, optional), comparison_period (dict, optional), group_by (list, optional)
2. AnalyzeLoanPortfolio - Requires: portfolio_data (dict), analysis_type (string, optional), comparison_data (dict, optional), risk_parameters (dict, optional)

Create a JSON plan with:
- goal: What we're trying to achieve
- steps: Array of steps, each with:
  - step: Step number
  - tool: Tool name to use
  - description: What this step does
  - inputs: Tool inputs (can reference previous outputs with ${{output_key}})
  - output_key: Key to store this step's output
- adaptations: Dictionary with keys:
  - error: What to do if a step fails
  - no_data: What to do if no data is available

IMPORTANT: If the query involves comparing time periods (e.g., "this quarter vs last year"), create separate steps to:
1. Get data for the current period
2. Get data for the comparison period
3. Analyze the comparison between periods

IMPORTANT: When using AnalyzeLoanPortfolio as the final step, set output_key to "analysis"

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
            
            # Ensure adaptations is a dictionary
            if "adaptations" not in plan or not isinstance(plan["adaptations"], dict):
                plan["adaptations"] = {
                    "error": "Provide general loan portfolio insights based on available data",
                    "no_data": "Explain what loan data would be needed for this analysis"
                }
            
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
                        "tool": "LoanQuery",
                        "description": "Get current period loan portfolio data",
                        "inputs": {
                            "query_type": "performance_metrics",
                            "time_period": {"quarter": "Q3", "year": 2025}
                        },
                        "output_key": "current_period_data"
                    },
                    {
                        "step": 2,
                        "tool": "LoanQuery",
                        "description": "Get comparison period loan data",
                        "inputs": {
                            "query_type": "performance_metrics",
                            "time_period": {"quarter": "Q3", "year": 2024}
                        },
                        "output_key": "comparison_period_data"
                    },
                    {
                        "step": 3,
                        "tool": "AnalyzeLoanPortfolio",
                        "description": "Compare loan performance between periods",
                        "inputs": {
                            "portfolio_data": "${current_period_data}",
                            "analysis_type": "performance_review",
                            "comparison_data": "${comparison_period_data}"
                        },
                        "output_key": "analysis"
                    },
                    {
                        "step": 4,
                        "tool": "LoanQuery",
                        "description": "Get loan risk distribution",
                        "inputs": {
                            "query_type": "risk_analysis",
                            "group_by": ["loan_type", "risk_tier"]
                        },
                        "output_key": "risk_data"
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
                        "tool": "LoanQuery",
                        "description": "Get loan portfolio summary data",
                        "inputs": {
                            "query_type": "portfolio_summary",
                            "filters": {}
                        },
                        "output_key": "portfolio_data"
                    },
                    {
                        "step": 2,
                        "tool": "AnalyzeLoanPortfolio",
                        "description": "Analyze loan portfolio composition and performance",
                        "inputs": {
                            "portfolio_data": "${portfolio_data}",
                            "analysis_type": "comprehensive"
                        },
                        "output_key": "analysis"
                    },
                    {
                        "step": 3,
                        "tool": "LoanQuery",
                        "description": "Get vintage performance data",
                        "inputs": {
                            "query_type": "vintage_analysis"
                        },
                        "output_key": "vintage_data"
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
                        "tool": "LoanQuery",
                        "description": "Get loan interest rate distribution",
                        "inputs": {
                            "query_type": "portfolio_summary",
                            "group_by": ["loan_type", "interest_rate_bucket"]
                        },
                        "output_key": "rate_data"
                    },
                    {
                        "step": 2,
                        "tool": "AnalyzeLoanPortfolio",
                        "description": "Analyze interest rate distribution and yield",
                        "inputs": {
                            "portfolio_data": "${rate_data}",
                            "analysis_type": "performance_review"
                        },
                        "output_key": "analysis"
                    },
                    {
                        "step": 3,
                        "tool": "LoanQuery",
                        "description": "Compare rates to market benchmarks",
                        "inputs": {
                            "query_type": "performance_metrics",
                            "filters": {"metric": "interest_rate"}
                        },
                        "output_key": "benchmark_data"
                    }
                ],
                "adaptations": {
                    "no_data": "Provide general interest rate guidance",
                    "error": "Explain typical rate ranges"
                }
            }
        
        else:
            # General loan portfolio analysis plan
            return {
                "goal": f"Provide loan portfolio insights for: {query}",
                "steps": [
                    {
                        "step": 1,
                        "tool": "LoanQuery",
                        "description": "Get comprehensive loan portfolio data",
                        "inputs": {
                            "query_type": "portfolio_summary"
                        },
                        "output_key": "portfolio_data"
                    },
                    {
                        "step": 2,
                        "tool": "LoanQuery",
                        "description": "Get loan risk metrics",
                        "inputs": {
                            "query_type": "risk_analysis"
                        },
                        "output_key": "risk_data"
                    },
                    {
                        "step": 3,
                        "tool": "AnalyzeLoanPortfolio",
                        "description": "Provide comprehensive portfolio analysis",
                        "inputs": {
                            "portfolio_data": "${portfolio_data}",
                            "analysis_type": "comprehensive",
                            "risk_parameters": "${risk_data}"
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